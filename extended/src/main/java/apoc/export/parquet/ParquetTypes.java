package apoc.export.parquet;


import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;

import static apoc.export.parquet.CustomTypes.*;
import static apoc.export.parquet.CustomConversions.*;

public enum ParquetTypes {
    POINT(new PointType(), new PointValueConversion(), new PointValueConversion()),
    DURATION(new DurationType(), new DurationValueConversion(), new DurationValueConversion()),
    NODE(new NodeType(), new NodeEntityConversion(), new NodeLoadConversion()),
    RELATIONSHIP(new RelationshipType(), new RelationshipEntityConversion(), new RelationshipLoadConversion());

    private final LogicalType type;
    private final Conversion writeConversion;
    private final Conversion readConversion;

    ParquetTypes(LogicalType type, Conversion conversion,Conversion readConversion) {
        this.type = type;
        this.writeConversion = conversion;
        this.readConversion = readConversion;
    }

    public LogicalType getType() {
        return type;
    }

    public Conversion getWriteConversion() {
        return writeConversion;
    }

    public Conversion getReadConversion() {
        return readConversion;
    }
}
