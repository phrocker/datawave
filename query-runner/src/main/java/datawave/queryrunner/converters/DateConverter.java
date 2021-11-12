package datawave.queryrunner.converters;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;

public class DateConverter implements IStringConverter<Instant> {
    private static DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder().appendPattern("MMddyyyy[ [HH][:mm][:ss][.SSS]]")
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0).parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter();
    
    @Override
    public Instant convert(String value) {
        
        try {
            System.out.println(value);
            return LocalDateTime.parse(value, DATE_FORMAT).atOffset(ZoneOffset.UTC).toInstant();
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            throw new ParameterException("Invalid timestamp");
        }
    }
}
