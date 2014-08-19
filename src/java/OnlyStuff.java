package sparkquet;

import parquet.column.ColumnReader;
import parquet.filter.RecordFilter;
import parquet.filter.ColumnRecordFilter;
import parquet.filter.UnboundRecordFilter;
import parquet.filter.ColumnPredicates;

import static sparkquet.Document.MyDocument;

public class OnlyStuff implements UnboundRecordFilter {
    public RecordFilter bind(Iterable<ColumnReader> readers){
        return ColumnRecordFilter.column("category", ColumnPredicates.equalTo(MyDocument.Category.STUFF)).bind(readers);
    }
}

