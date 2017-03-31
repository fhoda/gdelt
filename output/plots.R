library(ggplot2)
full_data = read.csv('full_data.csv')

###### Total Violence & Peaceful Actions for all years
totals <- ggplot() +
  geom_line(data=full_data, aes(x=Year, y=Total_Violence), color='red') +
  geom_point(data=full_data, aes(x=Year, y=Total_Violence), color='red')

totals <- totals + 
  geom_line(data=full_data, aes(x=Year, y=Total_Peace), color='green') +
  geom_point(data=full_data, aes(x=Year, y=Total_Peace), color='green') + 
  scale_x_continuous(breaks=seq(1980,2016,2))

print(totals)



##########