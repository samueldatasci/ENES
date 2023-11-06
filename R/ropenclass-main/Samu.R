walmart_data <- read.csv("walmart_data.csv")

walmart_data

head(walmart_data, 10)
tail(walmart_data, 10)

walmart_data[1, 'Weekly_Sales']
walmart_data[, 'Weekly_Sales']
walmart_data[c(1,2), 'Weekly_Sales']
walmart_data[c(1,2), c('Weekly_Sales', 'Date')]
# The first parameter defines the row to filter on (row number); the second parameter defines the filter for columns.
# In both cases, we can use c() = combine() to pass more than one row or more than one column.

walmart_data$Weekly_Sales

colnames(walmart_data)

walmart_data[order(walmart_data$Weekly_Sales),]
?sort
?order

help( head)

install.packages("dplyr")
library(dplyr)


install.packages("tidyverse")
library(tidyverse)


# Pipe operation
walmart_data %>%
  arrange(Weekly_Sales)


walmart_data %>%
  group_by(Dept) %>%
  summarize( mean_sales = mean(Weekly_Sales))


walmart_data %>%
  group_by(Dept) %>%
  summarize( median_sales = median(Weekly_Sales))



agg_sales <- walmart_data %>%
  group_by(Date)  %>%
  summarise(sum_sales = sum(Weekly_Sales))



# How to present the data as a chart

install.packages('ggplot2')
library(ggplot2)


ggplot(
  data = agg_sales,
  aes(x=as.Date(Date), y=sum_sales, group=1)
) + geom_line( color='orange')

# aesthetics


# We could convert the Date field from string to Date in the actual dataframe:

agg_sales$Date <- as.Date( agg_sales$Date)



# How to build a forecast model based on the data that we have
install.packages('forecast')
library(forecast)

#Let's divide in 80/20
agg_sales

train_split = as.integer( nrow(agg_sales)*0.8)
train <- agg_sales[1:train_split,]
test <- agg_sales[train_split:nrow(agg_sales),]


walmart_ts <- ts(data=agg_sales$sum_sales, frequency=52)
ddata <- decompose(walmart_ts, 'additive')

plot(ddata)

arima_model <- auto.arima(train$sum_sales)
arima_model
summary(arima_model)
#plot(arima_model)


# Let's forecast the next 30 weeks

my_forecast <- forecast::forecast(arima_model, 30)
my_forecast <- as.data.frame(my_forecast)

y_pred <- my_forecast$`Point Forecast`
y_test <- test$sum_sales


plot_test <- ggplot(
  data = test,
  aes(x=Date, y=sum_sales, group=1)
) + geom_line()


plot_test + geom_line( data=my_forecast,
                       aes(x=test$Date, y=`Point Forecast`),
                       color='red')
