# This script merges all of the seperate yearly count files into one csv with rows for each year's data.
# It also adds a column for the year of the data.

full_data <- data.frame(ASSAULT =integer(),
                 CONSULT=integer(),
                 ENGAGE_IN_DIPLOMATIC_COOPERATION=integer(),
                 FIGHT=integer(),
                 PROVIDE_AID=integer(),
                 UNCONVENTIONAL_MASS_VIOLENCE=integer(),
                 YIELD=integer(),
                 Total_Peace=integer(),
                 Total_Violence =integer(),
                 Year = integer(),
                 stringsAsFactors=FALSE)



files <- list.files(path="./", pattern=".txt")
for (f in files){
  temp <- read.table(f)
  temp <- as.data.frame(t(temp))
  temp$year = gsub(".txt", "", f, fixed = TRUE)
  colnames(temp) <- c('ASSAULT', 
                      'CONSULT', 
                      'ENGAGE_IN_DIPLOMATIC_COOPERATION', 
                      'FIGHT', 
                      'PROVIDE_AID',
                      'UNCONVENTIONAL_MASS_VIOLENCE', 
                      'YIELD', 
                      'Total_Peace', 
                      'Total_Violence', 
                      'Year')
  full_data = rbind(full_data, temp[2,], make.row.names = FALSE)
}

write.csv(full_data, file = "full_data.csv", row.names = FALSE)




