package io.flatf.foundation.common.param;

import io.flatf.foundation.common.param.Params.ValueType;

/**
 * Inner Key type
 */
public interface JointKey extends ParamKey {

    int key0();

    int key1();

    ValueType getValueType();

}