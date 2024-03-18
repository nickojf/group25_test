# Libraries
library(gridExtra)
library(RSQLite) 
library(ggplot2)
library(dplyr)


# Connect to the database
my_db <- RSQLite::dbConnect(RSQLite::SQLite(), "database/e-commerce.db") 

# SQL Queries
sales_query <- "SELECT customer.customer_id, city, country, quantity, shipping.shipment_status, 
           price, brand, subcategory.subcategory_name, review_score, category.category_name, 
           product_name, product.product_id, promo_price, order_date
        FROM customer
        INNER JOIN address ON customer.customer_id = address.customer_id
        INNER JOIN `order` ON customer.customer_id = `order`.customer_id
        LEFT JOIN product ON product.product_id = `order`.product_id
        LEFT JOIN category ON category.category_id = product.category_id
        LEFT JOIN transaction_billing ON `order`.order_id = transaction_billing.order_id 
        LEFT JOIN shipping ON shipping.billing_id = transaction_billing.billing_id
        LEFT JOIN subcategory ON subcategory.category_id = category.category_id
        LEFT JOIN review ON review.customer_id = customer.customer_id
        LEFT JOIN promotion ON promotion.category_id = category.category_id"

seller_query <- "SELECT category_name, product_name, seller.seller_id, review_score
                 FROM product
                 LEFT JOIN provide ON product.product_id = provide.product_id
                 LEFT JOIN seller ON provide.seller_id = seller.seller_id
                 LEFT JOIN category ON product.category_id = category.category_id
                 LEFT JOIN review ON product.product_id = review.product_w_category$product_id"

# Data Extraction
sales_analysis <- dbGetQuery(my_db, sales_query)
seller <- dbGetQuery(my_db, seller_query)

# Sales Data Generation
sales_data <- sales_analysis %>% 
  mutate(sales_amount = price * quantity * ifelse(is.na(promo_price), 1, promo_price)) 

# Analysis

## Sales Trend by Category
category_sales <- sales_data %>% 
  group_by(category_name) %>% 
  summarise(sales_amount = sum(sales_amount))

viz1 <- ggplot(category_sales, aes(x = sales_amount, y = reorder(category_name, sales_amount), fill = sales_amount)) + 
  geom_col() + 
  geom_vline(xintercept = mean(category_sales$sales_amount), color = "red") + 
  labs(title = "Sales amount by Category") +
  theme_classic()

# Save the plot
this_filename_date <- as.character(Sys.Date())
# format the Sys.time() to show only hours and minutes 
this_filename_time <- as.character(format(Sys.time(), format = "%H_%M"))
ggsave(paste0("visualization/sales_trend_by_category_",
              this_filename_date,"_",
              this_filename_time,".png"))

## Geographical Sales 
stats_sales_city <- sales_data %>% 
  group_by(city) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  summarise(mean = mean(sales_amount), sd = sd(sales_amount))

sales_data %>% 
  group_by(country, city) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  mutate(color_condition = case_when(sales_amount > stats_sales_city$mean ~ "1. over the average",
                                     sales_amount > (stats_sales_city$mean - stats_sales_city$sd) ~ "2. slightly below the average", 
                                     sales_amount > (stats_sales_city$mean - 2 * stats_sales_city$sd) ~ "3. below the average")) %>%
  ggplot(aes(x = country, y = city, fill = color_condition)) +
  geom_tile() +
  scale_fill_manual(values = c("1. over the average" = "steelblue1", "2. slightly below the average" = "lightcyan3", "3. below the average" = "coral1")) +
  labs(title = "Geographical Sales Heatmap", subtitle = "(Average sales by cities = 34,293 pounds)") +
  theme_classic()

## Sales amount by reviews
sales_data %>% 
  group_by(category_name) %>% 
  summarise(average_review_score = mean(review_score), sales_amount = sum(sales_amount)) %>% 
  mutate(color = case_when(sales_amount > mean(category_sales$sales_amount) ~ "1. Over the average sales by category", 
                           sales_amount < mean(category_sales$sales_amount) ~ "2. Below the average sales by category")) %>%
  ggplot(aes(x = average_review_score, 
             y = reorder(category_name, average_review_score), 
             color = color, 
             fill = color)) + 
  geom_point(size = 4) +
  geom_col(width = 0.01) + 
  labs(title = "Sales amount by reviews", subtitle = "(Average sales by category = 75,444 pounds)", 
       x = "Average review score", y = "Category") +
  theme_classic()

## Sales trend by date
sales_data$order_date <- as.Date(sales_data$order_date)
sales_by_date <- sales_data %>% 
  group_by(order_date, category_name) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  mutate(year = year(order_date), 
         month = month(order_date)) %>% 
  mutate(year_month = sprintf("%04d-%02d", year, month))

# sales trend
ggplot(sales_by_date, aes(x = order_date, y = sales_amount)) + 
  geom_line() +
  labs(title = "Sales trend over time", subtitle = "(Average sales amount by date = 13,236 pounds)", x = "Time", y = "Total sales") +
  geom_hline(yintercept = mean(sales_by_date$sales_amount), color = "red") + 
  theme_classic()

# Disconnect from the database
dbDisconnect(my_db)

