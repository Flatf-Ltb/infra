/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package town.lost.oms.dto;

public enum BuySell {

    Buy(1),

    Sell(-1);

    public final int direction;

    BuySell(int direction) {
        this.direction = direction;
    }

}
