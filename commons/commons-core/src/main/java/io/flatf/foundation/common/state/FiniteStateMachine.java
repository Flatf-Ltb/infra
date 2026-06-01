package io.flatf.foundation.common.state;

public interface FiniteStateMachine {

	State getState();

	State handleSignal(Signal signal);

}
