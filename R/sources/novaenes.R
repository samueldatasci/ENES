
library(readr)
library(dplyr)
library(parquet)
library(car)

# Read in the parquet file
df <- read_parquet("dfInfoEscolas.parquet.gzip")

# Fit a linear regression model
model <- lm(CIF_bonus_median_ExamRounded ~ ., data = df)

# Check the betas and significance
summary(model)
