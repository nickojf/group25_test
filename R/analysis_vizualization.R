# Libraries
library(gridExtra)
library(RSQLite) 
library(ggplot2)
library(dplyr)
library(lubridate)


# Connection to the database
my_db <- RSQLite::dbConnect(RSQLite::SQLite(), "database/e-commerce.db") 

# Ensure figures directory exists
if (!dir.exists("figures")) {
  dir.create("figures")
}

# PART 1 - DATA ANALYSIS

# Function to generate filename with date-time suffix
  generate_filename <- function(prefix) {
  today_date <- as.character(Sys.Date())
  current_time <- format(Sys.time(), format = "%H_%M_%S")
  filename <- paste0("figures/", prefix, "_", today_date, "_", current_time, ".csv")
  return(filename)
}

# 1. Rank order value from highest to lowest
result1 <- dbGetQuery(my_db, "
SELECT 
    o.order_id,
    o.customer_id,
    SUM(o.quantity * p.price) AS total_value
FROM 
    'order' o
JOIN 
    product p ON o.product_id = p.product_id
GROUP BY 
    o.order_id, o.customer_id
ORDER BY 
    total_value DESC
LIMIT 10
")
write.csv(result1, generate_filename("rank_order_value"), row.names = FALSE)

# 2. Identify Customers with Most Orders
result2 <- dbGetQuery(my_db, "
SELECT 
    c.customer_id,
    c.first_name, 
    c.last_name, 
    COUNT(*) AS number_of_orders
FROM 
    'order' o
JOIN 
    customer c ON o.customer_id = c.customer_id
GROUP BY 
    c.customer_id
ORDER BY 
    number_of_orders DESC
LIMIT 5
")
write.csv(result2, generate_filename("customers_most_orders"), row.names = FALSE)

# 3. Identify the Most Profitable Products
result3 <- dbGetQuery(my_db, "
SELECT 
    p.product_name, 
    (o.quantity * p.price) AS total_amount_sold
FROM 
    'order' o
JOIN
    product p ON o.product_id = p.product_id
GROUP BY 
    p.product_name
ORDER BY 
    total_amount_sold DESC
LIMIT 5
")
write.csv(result3, generate_filename("most_profitable_products"), row.names = FALSE)

# 4. Identify Products with the Highest Review Ratings
result4 <- dbGetQuery(my_db, "
SELECT 
    p.product_id,
    p.product_name, 
    AVG(r.review_score) AS avg_review_rating
FROM 
    review r
JOIN 
    product p ON r.product_id = p.product_id
GROUP BY 
    p.product_name
ORDER BY 
    avg_review_rating DESC
LIMIT 5
")
write.csv(result4, generate_filename("products_highest_reviews"), row.names = FALSE)

# 5. Product Sales Rank
result5 <- dbGetQuery(my_db, "
SELECT 
    p.product_id,
    p.product_name,
    SUM(o.quantity) AS quantity_sold
FROM 
    'order' o
JOIN 
    product p ON o.product_id = p.product_id
GROUP BY 
    p.product_id, p.product_name
ORDER BY 
    quantity_sold DESC
")
write.csv(result5, generate_filename("product_sales_rank"), row.names = FALSE)

# 6. Category-wise Sales Analysis
result6 <- dbGetQuery(my_db, "
SELECT 
    c.category_id,
    c.category_name,
    COUNT(o.quantity) AS total_sold_unit
FROM 
    'order' o
JOIN 
    Product p ON o.product_id = p.product_id
JOIN 
    category c ON p.category_id = c.category_id
GROUP BY 
    c.category_id, c.category_name
ORDER BY 
    total_sold_unit DESC
")
write.csv(result6, generate_filename("category_wise_sales"), row.names = FALSE)

# PART 2 - DATA VISUALIZATION

# SQL Queries
sales_query <- " SELECT 
                  customer.customer_id, city, country, quantity, 
                  shipping.shipment_status, price, brand, subcategory.subcategory_name, 
                  review_score, category.category_name, product_name, product.product_id, 
                  promo_price, order_date
                FROM customer
                INNER JOIN address ON customer.customer_id = address.customer_id
                INNER JOIN `order`ON customer.customer_id = `order`.customer_id
                LEFT JOIN product ON product.product_id = `order`.product_id
                LEFT JOIN category ON category.category_id = product.category_id
                LEFT JOIN transaction_billing ON `order`.order_id = transaction_billing.order_id
                LEFT JOIN shipping ON shipping.billing_id = transaction_billing.billing_id
                LEFT JOIN subcategory ON subcategory.category_id = category.category_id
                LEFT JOIN review ON review.customer_id = customer.customer_id
                LEFT JOIN promotion ON promotion.category_id = category.category_id"

seller_query <- " SELECT 
                    category_name, product_name, seller.seller_id, review_score
                  FROM product
                  LEFT JOIN provide
                  ON product.product_id = provide.product_id
                  LEFT JOIN seller
                  ON provide.seller_id = seller.seller_id
                  LEFT JOIN category
                  ON product.category_id = category.category_id
                  LEFT JOIN review
                  ON product.product_id = review.product_w_category$product_id"

# Data Extraction
sales_analysis <- dbGetQuery(my_db, sales_query)
seller <- dbGetQuery(my_db, seller_query)

# Sales Data Generation
sales_data <- sales_analysis %>% mutate(sales_amount = price * quantity * ifelse(is.na(promo_price), 1, promo_price)) 

# Sales Trend by Category
# The average amount of sales by each category is 75,443.85 pounds. Amongst 10 product categories, the sales amount of automotive, 
# beauty_cosmetics, home & garden, pet_supplies and furniture are over the average sales amount by categories. 
category_sales <- sales_data %>% group_by(category_name) %>% summarise(sales_amount = sum(sales_amount))

viz1 <- ggplot(category_sales, aes(x = sales_amount, 
                                   y = reorder(category_name, sales_amount), 
                                   fill = sales_amount)) + 
        geom_col() + 
        geom_vline(aes(xintercept = mean(category_sales$sales_amount), color = "mean"), linetype = "dashed") +
        scale_color_manual(name = " ", values = c(mean = "red")) +
        labs(title = "Sales amount by Category", x = "Sales amount", y = "Category") +
        theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/sales_trend_by_category_", this_filename_date, "_", this_filename_time, ".png"), plot = viz1, device = "png", width = 10, height = 7)

# Sales per unit by category
# This table provides insights into the total units of products sold for each sub-category, organized under their respective parent categories. 
# Understanding sales performance at both sub-category and parent category levels is crucial for businesses to identify top-performing product groups, 
# optimize marketing strategies, and allocate resources effectively across different product categories.

# SQL Queries
top_query <- "SELECT 
                cat.category_id AS parent_category_id,
                cat.category_name AS parent_category_name,
                sub.subcategory_id,
                sub.subcategory_name,
                COUNT(ord.quantity) AS total_sold_units
              FROM `order` ord
              JOIN product prod ON ord.product_id = prod.product_id
              JOIN subcategory sub ON prod.category_id = sub.category_id
              JOIN category cat ON sub.category_id = cat.category_id
              GROUP BY cat.category_id, cat.category_name
              ORDER BY total_sold_units DESC"

# Data Extraction
top_categ <- dbGetQuery(my_db, top_query)

viz2 <- ggplot(top_categ, aes(x = parent_category_name, y = total_sold_units, fill = parent_category_name)) +
  geom_bar(stat = "identity") +
  labs(x = "category", y = "Total Sold Units", title = "Total Sales by category") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/sales_unit_by_category_", this_filename_date, "_", this_filename_time, ".png"), plot = viz2, device = "png", width = 10, height = 7)


# Geographical Sales 
# For the analysis of sales by regions, we used three scales. "Over the average" represents the amount of sales greater than the average sales amount by city. 
# "Slightly below the average" and "below the average" represent the amount of sales 1 and 2 standard deviation far from the average, respectively. 
# Geographically, Reading, Portsmouth, Leicester, and Brighton in England had the sales amount over the average sales by city, which is 44,378.74 pounds. 
# There was no city whose sales amount is below the average, but except for four cities having the sales over the average, the rest of the cities in England have the sales amount slightly below the average. 
# Interestingly, the sales of the cities in Scotland show two extreme results, where the sales of Glasgow is greater than average, and those of Edinburgh is below the average. 
# Wales which is Cardiff had the sales over the average. Unsurprisingly, England positions in the first place in total sales amount by country, following by Wales and Scotland.
stats_sales_city <- sales_data %>% 
  group_by(city) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  summarise(mean = mean(sales_amount), sd = sd(sales_amount))

viz3 <- sales_data %>% group_by(country, city) %>% summarise(sales_amount = sum(sales_amount)) %>% 
  mutate(color_condition = case_when(sales_amount > stats_sales_city$mean ~ "1. Over the average",
                                     sales_amount > (stats_sales_city$mean - stats_sales_city$sd) ~ "2. Slightly below the average", 
                                     sales_amount > (stats_sales_city$mean - 2 * stats_sales_city$sd) ~ "3. Below the average")) %>%
  ggplot(aes(x = country, y = city, fill = color_condition)) +
  geom_tile() +
  scale_fill_manual(values = c("1. Over the average" = "steelblue1", "2. Slightly below the average" = "lightcyan3", "3. Below the average" = "coral1")) +
  labs(title = "Geographical Sales Heatmap") +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/geographical_sales_heatmap_", this_filename_date, "_", this_filename_time, ".png"), plot = viz3, device = "png", width = 10, height = 7)

# sales amount by country
viz4 <- sales_data %>% group_by(country) %>% summarise(Total_sales_amount = sum(sales_amount)) %>% 
  ggplot(aes(x = country, y = Total_sales_amount, fill = Total_sales_amount)) + geom_col() + labs(title = "Sales by country") + 
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/sales_by_country_", this_filename_date, "_", this_filename_time, ".png"), plot = viz4, device = "png", width = 10, height = 7)

# Sales amount by reviews
# A higher review score did not mean the high amount of sales. As shown in the plot, clothing had the sales amount below the average sales by category, 
# however, home & garden which had slightly less review score than clothing showed better sales performance. Automotive had the least review score, 
# but displayed the sales greater than the average sales by category.
viz5 <- sales_data %>% 
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
  labs(title = "Category by reviews", subtitle = "(Average sales by category = 75,444 pounds)", 
       x = "Average review score", y = "Category") +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/category_by_review_", this_filename_date, "_", this_filename_time, ".png"), plot = viz5, device = "png", width = 10, height = 7)

# Sales trend by date
# The sales shows the increasing trend over time from 2023 
# to February of 2024. Overall, total sales fluctuates frequently, showing numerous ups and downs. 
# Also, the time-series data appears increasing variances and nearly 4 cycles over time. 
sales_data$order_date <- as.Date(sales_data$order_date)
sales_by_date <- sales_data %>% 
  group_by(order_date, category_name) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  mutate(year = year(order_date), 
         month = month(order_date)) %>% 
  mutate(year_month = sprintf("%04d-%02d", year, month))

# Sales trend
viz6 <- ggplot(sales_by_date, aes(x = order_date, y = sales_amount)) + geom_line() +
  geom_smooth(method = lm, alpha = 0.3, aes(color = "Trend line")) + 
  labs(title = "Sales trend over time", subtitle = "(Average sales amount by date = 13,236 pounds)", x = "Time", y = "Total sales") +
  scale_colour_manual(name=" ", values=c("blue")) +
  geom_hline(aes(yintercept = mean(sales_by_date$sales_amount), linetype = "Average sales by date"), color = "red") + 
  scale_linetype_manual(values = 2) +
  labs(linetype = NULL) +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/sales_trend_over_time_", this_filename_date, "_", this_filename_time, ".png"), plot = viz6, device = "png", width = 10, height = 7)

# Total sales by year
viz7 <-sales_by_date %>% group_by(year_month) %>% ggplot(aes(x = sales_amount, y = year_month)) + 
  geom_col() +
  labs(title = "Total sales by year and month", x = "Total sales", y = "Year and Month") +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/total_sales_by_year_and_month_", this_filename_date, "_", this_filename_time, ".png"), plot = viz7, device = "png", width = 10, height = 7)

# Total sales per category
sales_category_query <- "SELECT 
                          o.order_date, 
                          c.category_id, 
                          c.category_name, 
                          SUM(o.quantity) AS units_sold
                          FROM `order` o
                          INNER JOIN product p ON o.product_id = p.product_id
                          INNER JOIN category c ON p.category_id = c.category_id
                          GROUP BY o.order_date, c.category_id, c.category_name
                          ORDER BY o.order_date ASC"

sales_category_data <- dbGetQuery(my_db, sales_category_query)
sales_category_data$order_date <- as.Date(sales_category_data$order_date)

viz8 <- ggplot(sales_category_data, aes(x = order_date, y = units_sold, color = category_name)) +
  geom_line() +
  labs(x = "Order Date", y = "Units Sold", title = "Units Sold by Category Across Time") +
  scale_color_discrete(name = "Category") +
  facet_wrap(~ category_name, scales = "free_y", ncol = 2)

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/unit_sold_by_category_", this_filename_date, "_", this_filename_time, ".png"), plot = viz8, device = "png", width = 10, height = 7)

# Price Distribution
price_distribution_query <- "SELECT
                              CASE
                              WHEN price BETWEEN 0 AND 99 THEN '0-99'
                              WHEN price BETWEEN 100 AND 199 THEN '100-199'
                              WHEN price BETWEEN 200 AND 299 THEN '200-299'
                              WHEN price BETWEEN 300 AND 399 THEN '300-399'
                              WHEN price BETWEEN 400 AND 499 THEN '400-499'
                              WHEN price BETWEEN 500 AND 599 THEN '500-599'
                              WHEN price BETWEEN 600 AND 699 THEN '600-699'
                              WHEN price BETWEEN 700 AND 799 THEN '700-799'
                              WHEN price BETWEEN 800 AND 899 THEN '800-899'
                              WHEN price BETWEEN 900 AND 999 THEN '900-999'
                              ELSE '1000+' END AS price_range,
                              COUNT(*) AS product_count
                              FROM product_data
                              GROUP BY price_range
                              ORDER BY price_range"

price_distribution <- dbGetQuery(my_db, price_distribution_query)

viz9 <- ggplot(price_distribution, aes(x = price_range, y = product_count)) +
  geom_bar(stat = "identity", color = "black", fill = "grey") + # Use a single fill color
  theme_minimal() +
  labs(x = "Price Range ($)", y = "Number of Products", title = "Product Price Distribution") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/price_distribution_", this_filename_date, "_", this_filename_time, ".png"), plot = viz9, device = "png", width = 10, height = 7)


# Customer per City
city_counts_query <- "SELECT 
                        a.city, 
                        COUNT(c.customer_id) AS num_customer_id
                      FROM customer c
                      JOIN address a ON c.customer_id = a.customer_id
                      GROUP BY a.city
                      ORDER BY num_customer_id DESC"

city_counts <- dbGetQuery(my_db, city_counts_query)

viz10 <- ggplot(city_counts, aes(x = reorder(city, -num_customer_id), y = num_customer_id, fill = city)) +
  geom_bar(stat = "identity") +
  labs(x = "City", y = "Number of Customers",
       title = "Number of Customers in Each City") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "none") 

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/number_of_customer_each_city_", this_filename_date, "_", this_filename_time, ".png"), plot = viz10, device = "png", width = 10, height = 7)


# Disconnect from the database
dbDisconnect(my_db)



