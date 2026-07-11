---- MODULE RafterInvariantDetectorNegative ----

VARIABLE value

Init == value = 0

Next == value' = 1

ExpectedViolation == value = 0

Spec == Init /\ [][Next]_<<value>>

====
