package shopping

type Item struct {
	Id    int `json:"id"`
	Price int `json:"price"`
	Stock int `json:"stock"`
}

type LoginJson struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type Cart struct {
	// if total < 0, then is a order
	Id         string      `json:"id"`
	Items      []ItemCount `json:"items"`
	TotalPrice int         `json:"total"`
}

type ItemCount struct {
	ItemId int `json:"food_id"`
	Count  int `json:"count"`
}

type Order Cart

// for GET /admin/orders
type OrderDetail struct {
	Id         string      `json:"id"`
	UserId     int         `json:"user_id"`
	Items      []ItemCount `json:"items"`
	TotalPrice int         `json:"total"`
}

type CartIdJson struct {
	CartId int `json:"cart_id"`
}

type UserIdAndPass struct {
	Id       int
	Password string
}
