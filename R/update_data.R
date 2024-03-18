# Libraries
library(tidyverse)
library(gridExtra)
library(RSQLite) 

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

ggplot(category_sales, aes(x = sales_amount, y = reorder(category_name, sales_amount), fill = sales_amount)) + 
  geom_col() + 
  geom_vline(xintercept = mean(category_sales$sales_amount), color = "red") + 
  labs(title = "Sales amount by Category") +
  theme_classic()

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

# Disconnect from the database
dbDisconnect(db)
