//! Framing of TLC tool output into complete `STARTMSG`/`ENDMSG` records.
//!
//! One reader requires the whole stream to be well framed; the other accepts
//! the longest complete prefix so a truncated or still-running transcript
//! still yields the frames it did finish.

use super::Frame;

pub(super) fn parse_frames(source: &str, allow_trailing_frame: bool) -> Result<Vec<Frame>, String> {
    let mut frames = Vec::new();
    let mut current: Option<(u16, u8, Vec<&str>)> = None;
    for line in source.lines() {
        if let Some(header) = line
            .strip_prefix("@!@!@STARTMSG ")
            .and_then(|line| line.strip_suffix(" @!@!@"))
        {
            if current.is_some() {
                return Err("nested TLC tool frame".to_owned());
            }
            let (code, class) = header
                .split_once(':')
                .ok_or("malformed TLC tool frame header")?;
            current = Some((
                code.parse().map_err(|_| "invalid TLC message code")?,
                class.parse().map_err(|_| "invalid TLC message class")?,
                Vec::new(),
            ));
            continue;
        }
        if let Some(footer) = line
            .strip_prefix("@!@!@ENDMSG ")
            .and_then(|line| line.strip_suffix(" @!@!@"))
        {
            let (code, class, body) = current
                .take()
                .ok_or("TLC tool frame ended without a start")?;
            if footer.parse::<u16>().ok() != Some(code) {
                return Err("TLC tool frame code mismatch".to_owned());
            }
            frames.push(Frame {
                code,
                class,
                body: body.join("\n"),
            });
            continue;
        }
        if let Some((_, _, body)) = current.as_mut() {
            body.push(line);
        }
    }
    if current.is_some() && !allow_trailing_frame {
        return Err("truncated TLC tool frame".to_owned());
    }
    if frames.is_empty() {
        return Err("TLC output contained no tool frames".to_owned());
    }
    Ok(frames)
}

pub(super) fn parse_frame_prefix(source: &str) -> Result<Vec<Frame>, String> {
    let mut frames = Vec::new();
    let mut current: Option<(u16, u8, Vec<&str>)> = None;
    for line in source.lines() {
        if let Some(header) = line
            .strip_prefix("@!@!@STARTMSG ")
            .and_then(|line| line.strip_suffix(" @!@!@"))
        {
            if current.is_some() {
                break;
            }
            let Some((code, class)) = header.split_once(':') else {
                break;
            };
            let (Ok(code), Ok(class)) = (code.parse(), class.parse()) else {
                break;
            };
            current = Some((code, class, Vec::new()));
            continue;
        }
        if let Some(footer) = line
            .strip_prefix("@!@!@ENDMSG ")
            .and_then(|line| line.strip_suffix(" @!@!@"))
        {
            let Some((code, class, body)) = current.take() else {
                break;
            };
            if footer.parse::<u16>().ok() != Some(code) {
                break;
            }
            frames.push(Frame {
                code,
                class,
                body: body.join("\n"),
            });
            continue;
        }
        if let Some((_, _, body)) = current.as_mut() {
            body.push(line);
        }
    }
    if frames.is_empty() {
        return Err("TLC output contained no complete tool-frame prefix".to_owned());
    }
    Ok(frames)
}
