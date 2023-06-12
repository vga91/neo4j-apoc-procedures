package apoc.export.parquet;



public enum ParquetTypes {
    // TODO ...
    POINT(new ParquetUtil.PointType(), new ParquetUtil.PointValueConversion()),
    DURATION(new ParquetUtil.DurationType(), new ParquetUtil.DurationValueConversion());

    private final ParquetUtil.CustomType pointType;
    private final ParquetUtil.CustomConversion pointConversion;

    ParquetTypes(ParquetUtil.CustomType pointType, ParquetUtil.CustomConversion pointConversion) {
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
