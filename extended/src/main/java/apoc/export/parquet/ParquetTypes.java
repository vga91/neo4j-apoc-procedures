package apoc.export.parquet;


import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;

public enum ParquetTypes {
    // TODO ...
    POINT(new ParquetUtil.PointType(), new ParquetUtil.PointValueConversion(), new ParquetUtil.PointValueConversion()),
    DURATION(new ParquetUtil.DurationType(), new ParquetUtil.DurationValueConversion(), new ParquetUtil.DurationValueConversion()),
    NODE(new ParquetUtil.NodeType(), new ParquetUtil.NodeEntityConversion(), new ParquetUtil.NodeLoadConversion()),
    RELATIONSHIP(new ParquetUtil.RelationshipType(), new ParquetUtil.RelationshipEntityConversion(), new ParquetUtil.RelationshipLoadConversion());

    private final LogicalType type;
    private final Conversion conversion;
    private final Conversion loadConversion;

    ParquetTypes(LogicalType type, Conversion conversion,Conversion loadConversion) {
        this.type = type;
        this.conversion = conversion;
        this.loadConversion = loadConversion;
    }

    public LogicalType getType() {
        return type;
    }

    public Conversion getConversion() {
        return conversion;
    }

    public Conversion getLoadConversion() {
        return loadConversion;
    }
}
