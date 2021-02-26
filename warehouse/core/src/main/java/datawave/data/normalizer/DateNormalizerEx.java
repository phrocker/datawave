package datawave.data.normalizer;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateNormalizerEx extends DateNormalizer {
    
    public static final DateNormalizerEx INSTANCE = new DateNormalizerEx();
    
    private static final long serialVersionUID = -2952967353332289586L;
    private static final Logger log = LoggerFactory.getLogger(DateNormalizerEx.class);
    
    public static final String[] EX_FORMAT_STRINGS;
    
    static {
        EX_FORMAT_STRINGS = new String[FORMAT_STRINGS.length + 1];
        EX_FORMAT_STRINGS[0] = "yyyy-MM-dd'T'HH:mm:ss";
        System.arraycopy(FORMAT_STRINGS, 0, EX_FORMAT_STRINGS, 1, FORMAT_STRINGS.length);
    }
    
    public String normalize(String fieldValue) {
        Date fieldDate = parseToDate(fieldValue);
        return parseToString(fieldDate);
    }
    
    private Date parseToDate(String fieldValue) {
        try {
            Date date = parseDate(fieldValue, EX_FORMAT_STRINGS);
            if (sanityCheck(date.getTime())) {
                return date;
            }
        } catch (ParseException e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to normalize value using DateUtils: " + fieldValue);
            }
        }
        
        // see if fieldValue looks like a Long value
        try {
            boolean valid = true;
            int size = fieldValue.length();
            long dateLong = 0;
            for (int i = 0; i < size; i++) {
                char c = fieldValue.charAt(i);
                if (c >= '0' && c <= '9') {
                    dateLong *= 10;
                    dateLong += (c - '0');
                } else {
                    valid = false;
                    break;
                }
            }
            if (valid && sanityCheck(dateLong)) {
                return new Date(dateLong);
            }
        } catch (NumberFormatException e) {
            // well, it's not a long
        }
        
        throw new IllegalArgumentException("Failed to normalize value as a Date: " + fieldValue);
        
    }
    
    private boolean sanityCheck(Long dateLong) {
        // between 1900/01/01 and 2100/12/31
        return -2208970800000L <= dateLong && dateLong < 4133894400000L;
    }
    
    private Collection<String> formatAll(Date date) {
        List<String> list = Lists.newArrayList();
        for (String format : EX_FORMAT_STRINGS) {
            DateFormat fs = getParser(format);
            String formatted = fs.format(date);
            if (formatted != null && !formatted.isEmpty()) {
                list.add(formatted);
            }
        }
        return list;
    }
    
    @Override
    public Date denormalize(String in) {
        return parseToDate(in);
    }
    
    @Override
    public Collection<String> expand(String dateString) {
        Date date = parseToDate(dateString);
        if (date != null && this.sanityCheck(date.getTime())) {
            return formatAll(date);
        }
        return Collections.emptyList();
    }
}
