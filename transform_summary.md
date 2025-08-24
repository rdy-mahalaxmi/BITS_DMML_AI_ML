## Dataset: churn_prepared_transformed
- Total rows: 7043
- Total columns: 34
- Derived Features:
  - TotalSpend = tenure * MonthlyCharges
  - TenureYears = tenure / 12
  - AvgMonthlySpend = TotalSpend / tenure
- Scaled numeric features using StandardScaler
- Encoded categorical columns using LabelEncoder

## Dataset: users_prepared_transformed
- Total rows: 10
- Total columns: 64
- Derived Features:
- Scaled numeric features using StandardScaler
- Encoded categorical columns using LabelEncoder

