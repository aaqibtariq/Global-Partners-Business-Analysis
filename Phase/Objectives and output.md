# Global-Partners-Business-Analysis


# Objective 

Company wants a unified data view across restaurant locations and ordering platforms to understand:

- **customer behavior**
- **spending patterns**
- **overall business performance**
- **daily CLV**
- **customer value tiers**
- **RFM segmentation**
- **churn risk**
- **sales trends and seasonality**
- **loyalty vs non-loyalty performance**
- **best/worst locations**
- **pricing and promotion impact**

That means the final platform should answer questions like:

- **Who are our best customers?**
- **Which customers are about to stop ordering?**
- **Are loyalty customers actually more valuable?**
- **Which stores perform best?**
- **Are discounts helping or hurting?**
- **What are the strongest time patterns in the business?**


This project is really solving three business problems at once.

## Problem 1: Customer value visibility

The company wants to know how much each customer is worth and how that changes daily. That is the CLV/LTV requirement.

## Problem 2: Customer lifecycle management

The company wants to segment customers and detect churn risk using RFM and inactivity behavior.

## Problem 3: Store and promotion performance

The company wants to compare stores, loyalty behavior, time patterns, and promotion effects.

So the project is both:

- **customer analytics**
- **business performance analytics**


# Output


## 1. EXECUTIVE PROOF — YOU BUILT A UNIFIED VIEW

### From main dashboard:

- Total Revenue: ~$10M
- Total Orders: 171K
- Customers: 20K
- At-Risk Customers: 17K
- VIP Customers: 1,119

## 2. CUSTOMER VALUE (CLV) 

Result clearly shows 

- CLV Segments: High / Medium / Low
- Top customers by spend
- Orders vs Spend relationship


## 3. RFM SEGMENTATION 

Result clearly shows 

RFM segments:

VIP
- Regular
- Churn Risk
- New Customers
- VIP = high frequency + high spend
- Churn Risk = low recency + low engagement

## 4. CHURN RISK 

We implemented:

- Days since last order
- Gap vs spend change
- Churn segmentation
- Customers with:
  - High days_since_last_order
  - Negative spend trend

## 5. SALES TRENDS & SEASONALITY

We implemented:

- Daily revenue trends
- Monthly revenue trends
- Category-level performance

- Revenue spikes over time
- Seasonality patterns (monthly variation)
- Category demand trends

## 6. STORE PERFORMANCE

we have

- Revenue by location
- Top 10 locations

- Ranked by total revenue
- Identified top and bottom performers

## 7. LOYALTY ANALYSIS

We have

- Revenue by loyalty vs non-loyalty
- Average order value comparison

- Loyalty customers:
  - Higher total revenue
  - Higher average order value

## 8. DISCOUNT IMPACT

No discounted transactions found


## This platform enables answering critical business questions:

- Identify high-value (VIP) customers using CLV and RFM segmentation
- Detect churn risk based on inactivity and spend decline
- Analyze customer lifecycle behavior across segments
- Evaluate store-level performance and revenue contribution
- Compare loyalty vs non-loyalty customer value
- Understand revenue trends and seasonality patterns
- Assess promotion and discount impact (framework implemented)
