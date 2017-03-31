library(ggplot2)
data = read.csv('full_data.csv')

###### Total Violence for all years
ggplot(data, aes(x=Year, y=Total_Violence)) +
  geom_line() +
  geom_point() + 
  scale_x_continuous(breaks=seq(1980,2016,2))



###### Total Peaceful actions for all years
# ggplot(data, aes(x=Year, y=Total_Peace)) +
#   geom_line() +
#   geom_point() +
#   scale_x_continuous(breaks=seq(1980,2016,2))


