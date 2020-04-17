# Lecture notes for 6.824

## Lecture 4:
VMWare replication: Primary backup doesn't write output to the output stream until the backup has had all the log entry up until the output command

## Lecture 8:
Linearizability: Observing the log output one can determine the linearizability (and consequently strong consistency). If the log contradicts the rule of linearizability, the system doesn't have strong consistency.