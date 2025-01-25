# README

## Project Summary

I designed and built a relational database given a number of raw datasets with PostGreSQL in order to solve a business problem and provide analysis.

The process included:

- Developing a Conceptual Data Model
- Exploration of Raw Data
- Designing a Normalized Schema
  - 1NF to 3NF
- Creating a Staging Database
- Implementing Normalized Schema
- Importing Transformed Data
- Performing Business-Related Queries
- Optimizing Queries with materialized Views

The paper details the process in addition to justifying the relational database for the selected business problem, discussing security recommendations, and scalability considerations.

## Tools Used

- PostgreSQL
- pgAdmin

## Schema Development Screenshots

#### Conceptual Model

![Conceptual Model](screenshots/Conceptual.png)

#### 1NF

![First Normal Form](screenshots/INF.png)

#### 2NF

![Second Normal Form](screenshots/2NF.png)

#### 3NF

![Third Normal Form](screenshots/3NF.png)

#### 3NF with Data Types

![Third Normal Form with Data Types](screenshots/3NF_with_datatypes.png)

#### 3NF Additional Table

![Third Normal Form with Added Table](screenshots/3NF_Scale.png)
