package shopping

// The type of ID for Item and User is int.
// To differ the two from others, we specify the
// field IDStr to indicate the type of ID is string,
// otherwise int.

type Item struct {
	ID    int `json:"id"`
	Price int `json:"price"`
	Stock int `json:"stock"`
}

type LoginJson struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type Order struct {
	// if total < 0, then is a order
	IDStr   string      `json:"id"`
	Items   []ItemCount `json:"items"`
	Total   int         `json:"total"`
	HasPaid bool        `json:"paid"`
}

type ItemCount struct {
	ItemID int `json:"item_id"`
	Count  int `json:"count"`
}

// for GET /admin/orders
type OrderDetail struct {
	Order
	UserID int `json:"user_id"`
}

type CartIDJson struct {
	IDStr string `json:"cart_id"`
}

type OrderIDJson struct {
	IDStr string `json:"order_id"`
}

type UserIDAndPass struct {
	ID       int
	Password string
}
