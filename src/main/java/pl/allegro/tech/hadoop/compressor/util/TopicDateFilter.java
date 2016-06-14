package pl.allegro.tech.hadoop.compressor.util;

import com.google.common.collect.Lists;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TopicDateFilter {
    private List<String> excludedDates;

    public TopicDateFilter(int delayInDays) {
        this.excludedDates = excludeDates(delayInDays);
    }

    public boolean shouldCompressTopicDir(String dir) {
        for (String excludedDate : excludedDates) {
            if (dir.contains(excludedDate)) {
                return false;
            }
        }
        return true;
    }

    private List<String> excludeDates(int days) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Calendar calendar = Calendar.getInstance();
        List<String> excludedDays = Lists.newArrayList();
        for (int i=0; i<days; i++) {
            Date time = calendar.getTime();
            excludedDays.add(dateFormat.format(time));
            calendar.add(Calendar.DATE, -1);
        }
        return excludedDays;
    }
}
