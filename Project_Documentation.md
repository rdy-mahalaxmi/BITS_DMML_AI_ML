# DATA MANAGEMENT FOR MACHINE LEARNING - ASSIGNMENT I
## End-to-End Data Management Pipeline for Customer Churn Prediction

**Group**: Group 89  
**Course**: Data Management for Machine Learning  
**Submission Date**: August 24, 2025  
**Weightage**: 20%

---

## 1. BUSINESS PROBLEM DEFINITION

### Problem Statement
Customer churn represents a critical business challenge where existing customers discontinue their relationship with a company, leading to significant revenue loss and increased operational costs. This project addresses **addressable churn** - scenarios where proactive intervention could prevent customer loss.

### Business Impact
- **Revenue Loss**: Financial institutions face potential 24% revenue decline over 3-5 years due to customer churn (PWC Research)
- **Acquisition Costs**: New customer acquisition is 5-25x more expensive than retention
- **Competitive Pressure**: Lost customers often migrate to competitors, potentially influencing other loyal customers

### Problem Scope
Develop a robust, automated data management pipeline to predict customer churn using telecommunications customer data, enabling proactive retention strategies.

---

## 2. KEY BUSINESS OBJECTIVES

### Primary Objectives
1. **Predictive Analytics**: Build accurate ML models to identify customers at high risk of churn
2. **Early Warning System**: Enable proactive intervention before customer departure
3. **Cost Optimization**: Reduce customer acquisition costs by improving retention
4. **Revenue Protection**: Minimize revenue loss through targeted retention strategies

### Technical Objectives
1. **Automated Pipeline**: Create end-to-end data management pipeline
2. **Scalable Architecture**: Design system for handling multiple data sources
3. **Data Quality**: Ensure high-quality, validated data for ML models
4. **Reproducibility**: Implement versioning and tracking for all pipeline components

### Success Metrics
- **Model Performance**: Achieve >85% accuracy in churn prediction
- **Pipeline Reliability**: >99% successful pipeline execution rate
- **Data Quality**: <5% data quality issues in processed datasets
- **Processing Time**: Complete pipeline execution within 30 minutes

---

## 3. DATA SOURCES AND ATTRIBUTES

### Primary Data Source: Telco Customer Churn Dataset
**Source**: GitHub Repository - Customer Churn Analysis  
**URL**: `https://raw.githubusercontent.com/SohelRaja/Customer-Churn-Analysis/master/Decision%20Tree/WA_Fn-UseC_-Telco-Customer-Churn.csv`  
**Format**: CSV  
**Size**: 7,043 customer records  

#### Customer Demographics
- `customerID`: Unique customer identifier
- `gender`: Customer gender (Male/Female)
- `SeniorCitizen`: Senior citizen status (0/1)
- `Partner`: Has partner (Yes/No)
- `Dependents`: Has dependents (Yes/No)

#### Service Information
- `tenure`: Number of months customer has stayed with company
- `PhoneService`: Has phone service (Yes/No)
- `MultipleLines`: Multiple phone lines (Yes/No/No phone service)
- `InternetService`: Internet service provider (DSL/Fiber optic/No)
- `OnlineSecurity`: Online security add-on (Yes/No/No internet service)
- `OnlineBackup`: Online backup add-on (Yes/No/No internet service)
- `DeviceProtection`: Device protection add-on (Yes/No/No internet service)
- `TechSupport`: Technical support add-on (Yes/No/No internet service)
- `StreamingTV`: TV streaming service (Yes/No/No internet service)
- `StreamingMovies`: Movie streaming service (Yes/No/No internet service)

#### Account Information
- `Contract`: Contract term (Month-to-month/One year/Two year)
- `PaperlessBilling`: Paperless billing (Yes/No)
- `PaymentMethod`: Payment method (Electronic check/Mailed check/Bank transfer/Credit card)
- `MonthlyCharges`: Monthly charges amount
- `TotalCharges`: Total charges amount

#### Target Variable
- `Churn`: Customer churn status (Yes/No) - **Primary prediction target**

### Secondary Data Source: User Demographics API
**Source**: JSONPlaceholder API  
**URL**: `https://jsonplaceholder.typicode.com/users`  
**Format**: JSON/REST API  
**Purpose**: Supplementary demographic data for enrichment

#### API Attributes
- `id`: User ID
- `name`: Full name
- `username`: Username
- `email`: Email address
- `address`: Geographic information
- `phone`: Phone number
- `website`: Website information
- `company`: Company details

---

## 4. EXPECTED PIPELINE OUTPUTS

### 4.1 Clean Datasets for Exploratory Data Analysis (EDA)
**Location**: `data/processed/`  
**Files**: 
- `churn_prepared.csv`: Cleaned main dataset
- `users_prepared.csv`: Cleaned API data

**Features**:
- Missing values handled (median imputation for numerical, mode for categorical)
- Data type standardization
- Outlier detection and treatment
- Duplicate removal

### 4.2 Transformed Features for Machine Learning
**Location**: `warehouse/`  
**Files**: 
- `X_train_[timestamp].csv`: Training features
- `X_test_[timestamp].csv`: Testing features  
- `y_train_[timestamp].csv`: Training labels
- `y_test_[timestamp].csv`: Testing labels

**Feature Engineering**:
- **Derived Features**:
  - `TotalSpend`: tenure × MonthlyCharges
  - `TenureYears`: tenure ÷ 12
  - `AvgMonthlySpend`: TotalSpend ÷ tenure
- **Encoded Variables**: One-hot encoding for categorical features
- **Scaled Features**: StandardScaler normalization for numerical features
- **Target Encoding**: Binary encoding for churn labels (Yes=1, No=0)

### 4.3 Deployable Models for Customer Churn Prediction
**Location**: `warehouse/`  
**Model Files**:
- `DecisionTree_model_[timestamp].pkl`
- `LogisticRegression_model_[timestamp].pkl`
- `RandomForest_model_[timestamp].pkl`

**Supporting Files**:
- `encoder_[timestamp].pkl`: Feature encoders
- `label_encoder_target_[timestamp].pkl`: Target encoders
- `feature_metadata_[timestamp].json`: Feature documentation

**Model Capabilities**:
- Real-time churn probability scoring
- Batch prediction processing
- Feature importance analysis
- Model performance monitoring

---

## 5. MEASURABLE EVALUATION METRICS

### 5.1 Model Performance Metrics
**Primary Metrics**:
- **Accuracy**: Overall prediction correctness (Target: >85%)
- **Precision**: True positive rate among predicted churners (Target: >80%)
- **Recall**: True positive rate among actual churners (Target: >75%)
- **F1-Score**: Harmonic mean of precision and recall (Target: >77%)

**Secondary Metrics**:
- **ROC-AUC**: Area under ROC curve (Target: >0.85)
- **Confusion Matrix**: Detailed classification breakdown
- **Feature Importance**: Top predictive features identification

### 5.2 Data Quality Metrics
**Data Validation**:
- **Completeness**: <5% missing values in final dataset
- **Consistency**: 100% data type compliance
- **Accuracy**: <2% data anomalies/outliers
- **Timeliness**: Data freshness within 24 hours

**Pipeline Quality**:
- **Reliability**: >99% successful pipeline runs
- **Performance**: <30 minutes end-to-end execution
- **Reproducibility**: 100% consistent results with same input

### 5.3 Business Impact Metrics
**Operational Metrics**:
- **Processing Volume**: Handle 10,000+ customer records
- **Scalability**: Support 100% data volume increase
- **Availability**: 99.9% system uptime
- **Response Time**: <5 seconds for real-time predictions

**ROI Metrics**:
- **Cost Savings**: Quantify retention vs. acquisition costs
- **Revenue Protection**: Measure prevented churn value
- **Efficiency Gains**: Reduce manual analysis time by 80%

---

## 6. TECHNICAL ARCHITECTURE

### 6.1 Pipeline Architecture
```
Raw Data → Ingestion → Storage → Validation → Preparation → Transformation → Feature Store → Model Training → Deployment
```

### 6.2 Technology Stack
- **Pipeline Orchestration**: Prefect
- **Data Processing**: Python, Pandas, Scikit-learn
- **Storage**: Local filesystem (data lake structure)
- **Database**: SQLite
- **Model Tracking**: MLflow
- **Visualization**: Matplotlib, Seaborn
- **Version Control**: Timestamp-based versioning

### 6.3 Data Flow
1. **Ingestion**: Automated data fetching from CSV and API sources
2. **Storage**: Partitioned data lake with source and timestamp organization
3. **Validation**: Comprehensive data quality checks and reporting
4. **Preparation**: Data cleaning, missing value handling, and EDA
5. **Transformation**: Feature engineering and database storage
6. **Feature Store**: Versioned feature management and metadata tracking
7. **Model Training**: Multi-algorithm training with performance evaluation
8. **Orchestration**: End-to-end pipeline automation with monitoring

---

## 7. DELIVERABLES CHECKLIST

### ✅ Completed Deliverables
- [x] End-to-end data pipeline implementation
- [x] Multiple data source ingestion (CSV + API)
- [x] Comprehensive data validation and quality reporting
- [x] Advanced feature engineering and transformation
- [x] Custom feature store with versioning
- [x] Multiple ML models (Decision Tree, Logistic Regression, Random Forest)
- [x] Pipeline orchestration with Prefect
- [x] Comprehensive logging and monitoring
- [x] Data versioning and reproducibility

### 📝 Documentation Status
- [x] **This Document**: Business problem, objectives, data sources, outputs, metrics ✅
- [x] Source code documentation and comments
- [x] Feature metadata and versioning
- [x] Pipeline architecture and design

### 🎯 Next Steps
- [ ] Create video walkthrough (5-10 minutes)
- [ ] Final testing and validation
- [ ] Package for submission

---

**Document Version**: 1.0  
**Last Updated**: August 24, 2025  
**Authors**: Group 89  
**Status**: Complete ✅
