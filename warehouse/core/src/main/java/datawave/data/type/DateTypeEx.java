package datawave.data.type;

import java.util.Date;

import datawave.data.normalizer.DateNormalizerEx;

public class DateTypeEx extends BaseType<Date> {
    
    private static final long serialVersionUID = 6435959460969312483L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF + PrecomputedSizes.DATE_STATIC_REF + Sizer.REFERENCE;
    
    public DateTypeEx() {
        super(DateNormalizerEx.INSTANCE);
    }
    
    public DateTypeEx(String dateString) {
        super(DateNormalizerEx.INSTANCE);
        super.setDelegate(normalizer.denormalize(dateString));
    }
    
    @Override
    public String getDelegateAsString() {
        // the normalized form of the date preserves milliseconds
        return normalizer.normalizeDelegateType(getDelegate());
    }
    
    /**
     * One string, one date object, one reference to the normalizer
     *
     * @return
     */
    @Override
    public long sizeInBytes() {
        return STATIC_SIZE + (2 * normalizedValue.length());
    }
}
