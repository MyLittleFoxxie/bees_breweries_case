# bees_breweries_case

removed street row (reduntant with address 1)

Phone Number Standardization:
Removes all non-numeric characters except '+'
Removes leading '+' and '1' if present
Handles null values properly
Example: "+ 351 289 815 218" → "351289815218"

Website URL Cleaning:
Removes escaped forward slashes
Example: "http:\/\/www.alewife.beer" → "http://www.alewife.beer"

Full Address Creation:
Combines all address fields into a single, properly formatted string
Handles null values in address_2 and address_3 gracefully
Example: "514 51st Ave, Long Island City, New York 11101-5879, United States"

Coordinate Validation:
Validates longitude is between -180 and 180
Validates latitude is between -90 and 90
Sets invalid coordinates to null
Properly casts valid coordinates to DoubleType

Created a sanitize_special_chars function that:
Normalizes text using NFKD normalization to decompose special characters
Replaces special characters with their ASCII equivalents
Replaces any remaining non-ASCII characters with underscores
Registered a UDF (User Defined Function) to use this sanitization in Spark
Applied the sanitization to the country, state, and city columns before writing
This change will ensure that special characters like umlauts (ä, ö, ü) and other non-ASCII characters are properly handled in the file paths. For example:
"Kärnten" will become "Karnten"
"Klagenfurt am Wörthersee" will become "Klagenfurt am Worthersee"