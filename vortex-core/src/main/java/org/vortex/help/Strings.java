package org.vortex.help;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Strings {

    public static final Pattern PATTERN = Pattern.compile("\\{(.+?)\\}");

    public static String format(String template, Map valueMap) {
        Matcher matcher = PATTERN.matcher(template);

        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            Object tokenValue = valueMap.get(matcher.group(1));
            String replacement = tokenValue == null ? "{" + matcher.group(1) + "}" : tokenValue.toString();
            matcher.appendReplacement(buffer, replacement);
        }

        matcher.appendTail(buffer);
        return buffer.toString();
    }

}
