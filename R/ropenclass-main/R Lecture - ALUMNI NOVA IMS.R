# This Script contains the code-along script we will 
# follow throughout the NOVA IMS ALUMNI lecture, on the 20th July 2023

# Load the walmart_data and walmart_features files
# Let's start by loading the walmart_data and walmart_features
walmart_data = read.csv('./walmart_data.csv')
walmart_features = read.csv('./walmart_features.csv')

# Store the number of rows of walmart_data in a variable called n_rows
n_rows = nrow(walmart_data)

# Print the first 5 rows of the dataframe walmart_data
head(walmart_data, 5)

# Count the number of rows per store using the Table command and store the resulting object in R with the name rows_per_store
rows_per_store = table(walmart_data$Store)

# Convert rows_per_store to a data frame - Rewrite the object
rows_per_store = as.data.frame(rows_per_store)

# Medium Level Exercises

# Which store has the most rows? Retrieve this value using R code
rows_per_store[order(-rows_per_store$Freq),][1,]

# Sum the sales by store on walmart_data and store the data in a sum_by_store object
sum_by_store = aggregate(
  walmart_data$Weekly_Sales,
  by = list(walmart_data$Store),
  FUN=sum
)

# Rename the columns of the data frame produced above as store_number and total_sales
colnames(sum_by_store) <- c('store_number','total_sales')

# Plot bar plot using base r with sorting total sales from sales with most sales to stores least sales
# Give a title to the plot "Sales By Store" and color the bars darkgreen
# Ignore the x labels for now
barplot(sum_by_store[order(-sum_by_store$total_sales),]$total_sales,
        main = "Sales by Store",
        col = "darkgreen")

# Compute the mean of every column in walmart_features
sapply(walmart_features, mean)

# Create a new column in walmart features called standardized_cpi subtracting the mean and dividing by the standard deviation
# Note: CPI has NAS!
walmart_features$standardized_cpi = (
  (walmart_features$CPI - mean(walmart_features$CPI, na.rm=TRUE))/
    sd(walmart_features$CPI, na.rm=TRUE)
)

# Produce a line plot for sales of store number 1 for every department
# Add labels to x and y
# Hint: Check the function lines
store_1 = walmart_data[walmart_data$Store==1,]

store_1_total = aggregate(
  store_1$Weekly_Sales,
  list(store_1$Date),
  FUN=sum
)

plot(as.factor(store_1_total$Group.1), 
     store_1_total$x,
     xlab='Weekly Sales',
     ylab='Date')

lines(as.factor(store_1_total$Group.1), 
      store_1_total$x)


# Use GGPlot to plot the total sales per week for store 20
# Add points to your plot
# Use as.Date on the x value in aes to format the x-axis labels
store_20 = walmart_data[walmart_data$Store==20,]

store_20_sales = aggregate(
  store_20$Weekly_Sales,
  list(store_20$Date),
  FUN=sum
)

# Loading our first external library into R
library(ggplot2)

# Here, it's recommended that we drop the table name when building the aes
(
  ggplot(store_20_sales, 
         aes(x=as.Date(store_20_sales$Group.1), y=store_20_sales$x, group=1)) 
  + geom_line(color='orange') + geom_point(color='darkorange')
  + xlab('Date') + ylab('Sales')
)

# Plot the sales for the top 5 departments with more sales for store 2 with ggplot2 with different colors
# Plot a line per department
# Hint: Use the Group on aes to plot different time series

store_2 = walmart_data[walmart_data$Store==2, ]

sales_by_dept = aggregate(
  store_2$Weekly_Sales,
  list(store_2$Dept),
  FUN=sum
)

top_depts = sales_by_dept[order(-sales_by_dept$x),'Group.1'][1:5]

top_5_dept_sales = store_2[store_2$Dept %in% top_depts,]

ggplot(
  top_5_dept_sales, 
  aes(x=Date, y=Weekly_Sales, group=Dept, color=Dept)
) + geom_line()


# In the graph above, convert the Dept to a factor and the date to a date type column

ggplot(
  top_5_dept_sales, 
  aes(x=as.Date(Date), y=Weekly_Sales, group=Dept, color=as.factor(Dept))
) + geom_line()

# Let's fit our first machine learning model
library(dplyr)

# Group the data by Store and Date, then calculate the total sales and identify if it's a holiday
sales_by_week <- walmart_data %>% 
  group_by(Store, Date) %>%
  summarise(total_sales = sum(Weekly_Sales), Holiday = max(IsHoliday))

library(forecast)

# Select data for Store 1
sales_by_week.store_1 <- data.frame(sales_by_week)[sales_by_week$Store == 1,]

# Plot the time series of sales for Store 1 using ggplot
ggplot(
  data = sales_by_week.store_1,
  aes(x = as.Date(Date), y = total_sales, group=1)
) + geom_line()

# Divide the data into training and testing sets
train <- sales_by_week.store_1[1:as.integer(nrow(sales_by_week.store_1) * 0.75),]
test <- sales_by_week.store_1 %>%
  anti_join(train)

# Decompose the time series to observe the trend, seasonal, and residual components
walmart_ts <- ts(train$total_sales, frequency = 52) 
ddata <- decompose(walmart_ts, "additive")
plot(ddata)

# Fit an ARIMA (AutoRegressive Integrated Moving Average) model to the time series data
arima_model <- auto.arima(walmart_ts)
summary(arima_model)

# Forecast future sales for Store 1 using the ARIMA model
forecast_arima = forecast::forecast(arima_model, h=36)

# Convert the forecast results to a data frame for evaluation
df_arima = as.data.frame(forecast_arima)

# Create a function to calculate Mean Absolute Percentage Error (MAPE)
mape <- function(actual, pred){
  mape <- mean(abs((actual - pred)/actual))*100
  return (mape)
}

# Calculate MAPE between actual sales in the test set and the forecasted sales
mape(test$total_sales, df_arima$`Point Forecast`)

# Plot the test data for Store 1
plot_test_data <- ggplot(
  data = test,
  aes(x=1:36, total_sales , group=1)
) + geom_line()

# Add the forecasted sales from the ARIMA model to the plot
plot_test_data + geom_line(
  data = df_arima,
  aes(x=1:36, `Point Forecast`, group=1, color='red'), 
)

# Finally, let's just see how to fit a decision tree model
# on other type of data

mtcars_df <- mtcars

# Using caret
library(rpart)

model <- rpart(mpg ~ disp+hp+wt, data=train_df, control=rpart.control(cp=.0001))

library(rpart.plot)
rpart.plot(model)


# Let's tweak the hyperparameters of the decision tree
?rpart.control

# Using caret
library(rpart)

model <- rpart(mpg ~ disp+hp+wt, data=train_df, control=rpart.control(cp=.0001,
                                                                      minsplit=2,
                                                                      minbucket=2))

rpart.plot(model)

# Checking the predictions
predict(model, test_df)

prediction <- predict(model, test_df)

mse = sum((prediction - test_df$mpg)**2)

rmse = sqrt(sum((prediction - test_df$mpg)**2))

rmse

prediction - test_df$mpg

# Prediction on custom car
predict(model, data.frame(disp=70, hp=100, wt=2000))
