Feature: Pipeline cost attribution and SLA alert dedup
  Spec: docs/specs/spec-cost-sla.md

  Scenario: 1 GiB processed is billed at exactly 0.023 USD
    Given a pipeline run that processed 1 GiB in 10 seconds
    When the pipeline run is recorded
    Then the recorded cost is exactly 0.023 USD

  Scenario: a second SLA breach for the same pipeline run does not send a second alert
    Given a pipeline that breached its 60 second SLA by running for 120 seconds
    When the SLA breach is checked for the first time
    Then the breach is detected and an alert is sent
    When the same pipeline breaches its SLA again for the same run_date
    Then the breach is detected but the alert is deduped
