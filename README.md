![Predictiveworks.](https://raw.githubusercontent.com/skrusche63/spark-pref/master/images/predictiveworks.png)

**Predictiveworks.** is an open ensemble of predictive engines and has been made to cover a wide range of today's analytics requirements. **Predictiveworks.**  brings the power of 
predictive analytics to Elasticsearch. [More...](http://predictiveworks.eu)

## Reactive User Preference Engine

The User Preference Engine actually supports two different approaches to user preference computing:

### Purchase Behavior

The purchase transactions of a certain time period are evaluated. From these transaction data User-Item ratings
are computed by taking the purchase frequency of a certain product into account, per customer and for normalization 
purchases also with respect to all customers.

Ratings are computed by a simple heuristic algorithm and the respective method can easily be applied to almost 
e-commerce stores. These ratings describe an appropriate input for recommendation techniques such as matrix factorization.

### Customer Engagement

The customer engagement events of a certain time period and with respect to a specific product (item) are evaluated. Events such 
as *article read*, *browsed*, *placed in cart*, *placed in wishlist* and other events are specified by pre-defined scores. These scores 
are used in combination with detected event frequencies when it comes to compute the rating for a certain item.

The advantage of this approach is, that additional contextual information (event, time) is made available for User-Item preferences. The 
mechanism provided can easily be adapted to incorporate other situation information such as device, location and more.

User-Item preferences derived from customer engagement sequences are an appropriate input for [Context-Aware Analysis](https://github.com/skrusche63/spark-fm), e.g. 
to compute context-aware product recommendations.


 