package io.flatf.foundation.common.param;

import io.flatf.foundation.common.param.Params.ValueType;

/**
 * 
 * @author yellow013
 */
public interface ParamKey {

	int getParamId();

	String getParamName();

	ValueType getValueType();

}
