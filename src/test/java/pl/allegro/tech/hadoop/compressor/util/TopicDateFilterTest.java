package pl.allegro.tech.hadoop.compressor.util;

import org.junit.Test;

import java.io.IOException;
import java.util.Calendar;

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.YEAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopicDateFilterTest {

    @Test
    public void shouldCompressTopicWithoutDelay() throws IOException {
        //given
        int noDelay = 0;
        TopicDateFilter dateFilter = new TopicDateFilter(noDelay);

        //when
        boolean shouldCompress = dateFilter.shouldCompressTopicDir("/topics/mytopic/" + getTodayDir());

        //then
        assertTrue(shouldCompress);
    }

    @Test
    public void shouldCompressTopicWithOneDayDelay() throws IOException {
        //given
        int oneDayDelay = 1;
        TopicDateFilter dateFilter = new TopicDateFilter(oneDayDelay);

        //when
        boolean shouldCompressTodaysDir = dateFilter.shouldCompressTopicDir("/topics/mytopic/" + getTodayDir());
        boolean shouldCompressYesterdaysDir = dateFilter.shouldCompressTopicDir("/topics/mytopic/" + getYesterdayDir());

        //then
        assertFalse(shouldCompressTodaysDir);
        assertTrue(shouldCompressYesterdaysDir);
    }

    private String getTodayDir() {
        Calendar cal = Calendar.getInstance();
        return String.format("%4d/%02d/%02d", cal.get(YEAR), cal.get(MONTH) + 1, cal.get(DAY_OF_MONTH));
    }

    private String getYesterdayDir() {
        Calendar cal = Calendar.getInstance();
        cal.add(DAY_OF_MONTH, -1);
        return String.format("%4d/%02d/%02d", cal.get(YEAR), cal.get(MONTH) + 1, cal.get(DAY_OF_MONTH));
    }
}
