# -*- coding: utf-8 -*-

from __future__ import absolute_import

from conftest import (
    json_get, token_gen, item_gen,
    item_store, new_cart, make_order, pay_order)

def test_pay_order():

    uid, token = next(token_gen)
    cart_id = new_cart(token)
    item_items = [next(item_gen)]

    # make order success
    res = make_order(uid, token, cart_id, item_items)
    assert res.status_code == 200
    order_id = res.json()["order_id"]
    assert len(order_id) > 0

    res = pay_order(uid, token, order_id)
    assert res.status_code == 200

def test_make_order_cart_not_owned_error():
    uid, token1 = next(token_gen)
    cart_id1 = new_cart(token1)
    _, token2 = next(token_gen)

    res = make_order(uid, token2, cart_id1, [next(item_gen)])
    assert res.status_code == 401
    assert res.json() == {"code": "NOT_AUTHORIZED_TO_ACCESS_CART",
                          "message": u"无权限访问指定的篮子"}

    res = make_order(uid, token1, cart_id1, [next(item_gen)])
    assert res.status_code == 200

    order_id = res.json()["order_id"]
    res = pay_order(uid, token2, order_id)
    assert res.status_code == 401
    assert res.json() == {"code": "NOT_AUTHORIZED_TO_ACCESS_ORDER",
                          "message": u"无权限访问指定的订单"}
