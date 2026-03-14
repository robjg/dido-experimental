package dido.swagger.parser;

import java.lang.reflect.Type;

public interface SchemaWalker {

    interface Parent extends SchemaWalker {

        Value field(String fieldName);

        void complete();
    }

    interface Value extends SchemaWalker {

        void addType(Type type);

        void addReference(String reference);

        Parent nested();

        Value array();

    }

}
