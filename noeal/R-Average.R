n<-read.csv("C:\\Users\\1267979\\Downloads\\bquxjob.csv")
View(n)
library(dplyr)
d<-n %>%
  select (period_id,scenario_id, item_id, site_id, avg_price)%>%
  group_by(period_id,scenario_id, item_id, site_id) %>%
  arrange(., period_id)%>%
  summarize(mean(avg_price,na.rm=TRUE))
write.csv(d,"C:/Users/1267979/Downloads/nn.csv", row.names = FALSE)








