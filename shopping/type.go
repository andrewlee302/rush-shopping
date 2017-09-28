package shopping

// The type of Id for Item and User is int.
// To differ the two from others, we specify the
// field IdStr to indicate the type of Id is string,
// otherwise int.

type Item struct {
	Id    int `json:"id"`
	Price int `json:"price"`
	Stock int `json:"stock"`
}

type LoginJson struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type Order struct {
	// if total < 0, then is a order
	IdStr   string      `json:"id"`
	Items   []ItemCount `json:"items"`
	Total   int         `json:"total"`
	HasPaid bool        `json:"paid"`
}

type ItemCount struct {
	ItemId int `json:"food_id"`
	Count  int `json:"count"`
}

// for GET /admin/orders
type OrderDetail struct {
	Order
	UserId int `json:"user_id"`
}

type CartIdJson struct {
	IdStr string `json:"cart_id"`
}

type OrderIdJson struct {
	IdStr string `json:"order_id"`
}

type UserIdAndPass struct {
	Id       int
	Password string
}
