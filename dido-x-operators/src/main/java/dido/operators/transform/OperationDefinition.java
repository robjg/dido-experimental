package dido.operators.transform;

public interface OperationDefinition {

    Runnable prepare(OperationContext context);
}
