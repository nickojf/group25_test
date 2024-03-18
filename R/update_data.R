library(dplyr)
library(RSQLite)

# Connect to the database
db <- RSQLite::dbConnect(SQLite(), "database/ecommerce.db")

# Read the new address data
new_address_data <- read.csv("data_upload/address_data2.csv") 

# Data validation and update logic 
existing_address_ids <- dbGetQuery(db, "SELECT address_id FROM address") %>% pull()

# Filter for addresses with new address_id values
new_addresses <- filter(new_address_data, !address_id %in% existing_address_ids)

if (nrow(new_addresses) > 0) {
  # Append only the new addresses to the database
  dbWriteTable(db, "address", new_addresses, append = TRUE)
  print("New addresses appended to the database")
} else {
  print("No new addresses to append - all address_id values already exist.")
}

# Read the complete address table
complete_address_table <- dbGetQuery(db, "SELECT * FROM address") 

# Generate a timestamp
current_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# Export to CSV with timestamp in the data_upload folder
write.csv(complete_address_table, 
          file = paste0("data_update/address_table_", current_timestamp, ".csv"), 
          row.names = FALSE)  

# Disconnect from the database
dbDisconnect(db)