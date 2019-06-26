package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.AlgebraicPartial;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import java.util.List;

public class AlgebraicWindowMerger<AggType extends AlgebraicPartial> extends DistributiveWindowMerger<AggType> {

    public AlgebraicWindowMerger(int numChildren, List<Window> windows, List<AggregateFunction> aggFunctions) {
        super(numChildren, windows, aggFunctions);
    }
}
