Feature: SCD Type 2 history in dim_customer
  Spec: docs/specs/spec-scd2-dim-customer.md

  Background:
    Given the gold job has been run against the bronze fixtures

  Scenario: a segment change closes the prior dim_customer row
    Given customer cust_000000 changed segment from mass to student on 2024-01-31
    Then the mass row for cust_000000 has valid_to set and is_current false
    And the student row for cust_000000 is_current true

  Scenario: exactly one current row per customer
    Then every customer has exactly one current row in dim_customer

  Scenario: no overlapping validity ranges per customer
    Then no customer has overlapping validity ranges in dim_customer

  Scenario: reprocessing the same bronze data is byte-identical
    When the gold job is run again against the same bronze fixtures
    Then the dim_customer history hash is unchanged
