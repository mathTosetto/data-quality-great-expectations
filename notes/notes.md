## 1. Column-Level Checks
- **Schema Validation**
    - Ensure that the expected columns exist in the specified order in the table.
        - Expectation: `ExpectTableColumnsToMatchOrderedList`

- **Schema Set Match Validation**
    - Validate a set of columns in the table
        - Expectation: `ExpectTableColumnsToMatchSet`

- **Column Data Types**
    - Validate that each column has the expected data type (e.g., string, integer, float).
        - Expectation: `ExpectColumnValuesToBeOfType`

- **Null/Empty Values**
    - Check that certain columns do not contain null, empty, or missing values.
        - Expectation: `ExpectColumnValuesToNotBeNull`

- **Uniqueness**
    - Ensure that column values are unique where necessary, such as for primary keys.
        - Expectation: `ExpectColumnValuesToBeUnique`

- **Value Ranges**
    - Validate that numeric columns have values within acceptable ranges (e.g., age >= 0).
        - Expectation: `ExpectColumnValuesToBeBetween`

- **Membership Checks**
    - Validate that column values belong to a predefined set (e.g., status column contains only ["Active", "Inactive"]).
        - Expectation: `ExpectColumnValuesToBeInSet`

- **Regular Expression Patterns**
    - Ensure column values match a specific pattern (e.g., email addresses, phone numbers).
        - Expectation: `ExpectColumnValuesToMatchRegex`

---

## 2. Table-Level Checks
- **Row Count**
    - Ensure that the number of rows in the table falls within an expected range.
        - Expectation: `ExpectTableRowCountToBeBetween`

- **Presence of Duplicates**
    - Ensure that there are no duplicate rows in the table.
        - Expectation: `ExpectCompoundColumnsToBeUnique`

- **Referential Integrity**
    - Ensure relationships between tables are maintained (e.g., foreign keys reference valid primary keys).
    - Custom Validation: Write a script to check against another table or use a join validation.

---

## 3. Distribution and Statistics
- **Distribution of Values**
    - Validate that column values follow a known distribution (e.g., normal distribution, or specific quantiles).
        - Expectation: `ExpectColumnMedianToBeBetween`, `ExpectColumnQuantileValuesToBeBetween`
- **Column Aggregates**
    - Verify aggregates, such as averages, medians, and sums, fall within acceptable ranges.
        - Expectation: `ExpectColumnMeanToBeBetween`, `ExpectColumnSumToBeBetween`

---

## 4. Data Freshness and Timeliness
- **Timestamps**
    - Ensure that date/time columns fall within an acceptable range or have values within the last n days.
        - Expectation: `ExpectColumnValuesToBeBetween` (for date ranges)
- **Data Completeness**
    - Ensure that the data is up to date (e.g., last updated within a specific timeframe).
    - Custom Validation: Check the most recent date in the column.

---

## 5. Advanced and Custom Checks
- **Cross-Column Validation**
    - Validate relationships between columns (e.g., start_date < end_date).
    - Custom Expectation: Create a custom expectation to validate column relationships.
- **Anomalies and Outliers**
    - Check for anomalous or outlier values using statistical methods.
        - Expectation: Use quantile-based checks or custom logic.
- **Data Completeness**
    - Ensure no unexpected gaps exist in sequential data (e.g., time series data).
        - Expectation: Write custom expectations to check for missing values or breaks.