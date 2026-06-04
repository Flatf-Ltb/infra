package io.flatf.infra.actors.sample.supervision;

// Represents an arithmetic expression involving integer numbers
public interface Expression {

    Expression getLeft();

    Expression getRight();

    // Basic arithmetic operations that are supported by the ArithmeticService.
    // Every
    // operation except the constant value has a left and right side. For example
    // the addition in (3 * 2) + (6 * 6) has the left side (3 * 2) and the right
    // side (6 * 6).
    abstract class AbstractExpression implements Expression {

        private final Expression left;
        private final Expression right;
        private final String operator;

        protected AbstractExpression(Expression left, Expression right, String operator) {
            this.left = left;
            this.right = right;
            this.operator = operator;
        }

        public Expression getLeft() {
            return left;
        }

        public Expression getRight() {
            return right;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof AbstractExpression that))
                return false;
            if (!left.equals(that.left))
                return false;
            if (!operator.equals(that.operator))
                return false;
            return right.equals(that.right);
        }

        @Override
        public int hashCode() {
            int result = left.hashCode();
            result = 31 * result + right.hashCode();
            result = 31 * result + operator.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "(" + getLeft() + " " + operator + " " + getRight() + ")";
        }
    }

    final class Add extends AbstractExpression {
        public Add(Expression left, Expression right) {
            super(left, right, "+");
        }
    }

    final class Multiply extends AbstractExpression {
        public Multiply(Expression left, Expression right) {
            super(left, right, "*");
        }
    }

    final class Divide extends AbstractExpression {
        public Divide(Expression left, Expression right) {
            super(left, right, "/");
        }
    }

    record Const(int value) implements Expression {

        @Override
        public Expression getLeft() {
            return this;
        }

        @Override
        public Expression getRight() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Const oConst))
                return false;
            return value == oConst.value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
}

