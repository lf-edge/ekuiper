# Nexmark SQL 

## Create Stream

### Bid Stream

```sql
create stream bid (auction bigint,bidder bigint,price bigint,channel string,url string,datetime bigint,extra string) WITH ( datasource = "bid", FORMAT = "json")
```

### Person Stream

```sql
create stream person (id bigint,name string,emailAddress string,creditCard string,city string,state string,datetime bigint,extra string) WITH ( datasource = "person", FORMAT = "json")
```

### Auction Stream

```sql
create stream auction (id bigint,itemName string,description string,initialBid bigint,reserve bigint,datetime bigint,expires bigint,seller bigint,category bigint,extra string) WITH ( datasource = "auction", FORMAT = "json")
```

## Nexmark Query

### Q1

```sql
SELECT auction, 0.9 * price, bidder, datetime FROM bid;
```

### Q2

```sql
SELECT auction, price FROM bid WHERE MOD(auction, 3) > 0;
```

### Q3

```sql
SELECT person.name, person.city, person.state, auction.id FROM auction INNER JOIN person on auction.seller = person.id WHERE auction.category = 3 and (person.state = 'or' OR person.state = 'id' OR person.state = 'ca') group by TUMBLINGWINDOW(ss, 30);
```

### Q4

```sql
SELECT MAX(bid.price) AS final, auction.category FROM auction inner join bid WHERE auction.id = bid.auction AND bid.datetime BETWEEN auction.datetime AND auction.expires group by TUMBLINGWINDOW(ss, 30),auction.id, auction.category -> Q4Mem
SELECT q4Mem.category, AVG(q4Mem.final) FROM q4Mem GROUP BY TUMBLINGWINDOW(ss, 30), q4Mem.category;
```

### Q6

```sql
Select row_number() over (partition by auction.id, auction.seller order by bid.price desc),  auction.id, auction.seller,bid.price,bid.datetime from auction inner join bid on auction.id = bid.auction where bid.datetime >= auction.datetime and bid.datetime <= auction.expires group by tumblingWindow(ss,30)
```

### Q7

```sql
select max(price) as maxprice, window_end() as datetime from bid group by TUMBLINGWINDOW(ss,30) -> Q7Mem
SELECT bid.auction, bid.price, bid.bidder, bid.datetime, bid.extra from bid inner join q7Mem on bid.price = q7Mem.maxprice where bid.datetime < q7Mem.datetime group by TUMBLINGWINDOW(ss,30)
```

### Q8

```sql
select person.id,person.name,window_start() from person inner join auction on person.id = auction.seller group by TUMBLINGWINDOW(ss,30)
```

### Q9

```sql
select bid.auction,bid.bidder,bid.price, bid.datetime, bid.extra, row_number() over (partition by auction.id order by bid.price desc, bid.datetime asc) as rownum from auction inner join bid on auction.id = bid.auction and bid.datetime >= auction.datetime and bid.datetime <= auction.expires group by tumblingWindow(ss,30)
```

### Q10

```sql
SELECT auction, bidder, price, datetime, extra, format_time(datetime, 'yyyy-MM-dd') as f1, format_time(datetime, 'HH:mm') as f2 FROM bid
```