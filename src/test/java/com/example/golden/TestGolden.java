package test.java.com.example.golden;

import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.sn.parent.goldentools.component.GoldenRTDBDao;
import com.rtdb.model.DoubleData;


public class TestGolden {

    @Test
    public void pullOnce() throws Exception {
        // 1. 定义 tag
        String tag = "ljDCS.V4_DPU2021_SH0123_AALM1_PV";

        // 2. 定义时间区间
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = fmt.parse("2025-08-13 09:08:37");
        Date end   = fmt.parse("2025-08-13 10:08:37");

        // 3. 调用方法获取数据
        List<DoubleData> list = GoldenRTDBDao.getDoubleArchivedValuesByTag(tag, start, end);

        // 4. 打印结果
        System.out.println("Fetched size = " + (list == null ? 0 : list.size()));
        if (list != null) {
            for (int i = 0; i < Math.min(list.size(), 10); i++) {
                DoubleData d = list.get(i);
                System.out.printf("%s\t%s%n", d.getDateTime(), d.getValue());
            }
        }
    }
}
