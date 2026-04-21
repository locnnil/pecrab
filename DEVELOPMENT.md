# Development notes for the project

This document contains notes and ideas for the development of the project.
It is a living document that will be updated as the project progresses.

## Approach decisions

- **Dispute scope**: The disputes only will be valid if the referenced tx belongs to the same client making the dispute.
- **What is disputable**: Only deposits are disputable, withdrawals are assumed to not be disputable.
  - There is no such thing as a dispute for a dispute, or a dispute for a chargeback, or a dispute for a resolution.
- Applying disputes: When a dispute is applied, the amount of the disputed transaction will be moved from available to held, and the total will remain the same.
  - Client with non sufficient available funds to cover the dispute will not be allowed to be disputed, the dispute will be ignored.
- **Duplicate transactions**: If a transaction with the same tx id is encountered, it will be ignored and a warning will be logged


