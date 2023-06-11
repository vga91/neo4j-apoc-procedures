package apoc.export.parquet;

public enum CustomTypes {
    // TODO ...
    POINT(new ParquetUtil.PointType(), new ParquetUtil.PointValueConversion());

    private final ParquetUtil.CustomType pointType;
    private final ParquetUtil.CustomConversion pointConversion;

    CustomTypes(ParquetUtil.CustomType pointType, ParquetUtil.CustomConversion pointConversion) {
        this.pointType = pointType;
        this.pointConversion = pointConversion;
    }

    public ParquetUtil.CustomType getType() {
        return pointType;
    }

    public ParquetUtil.CustomConversion getConversion() {
        return pointConversion;
    }
}
