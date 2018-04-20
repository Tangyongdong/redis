package com.tyd.redis;

import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisApplicationTests {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    public void contextLoads() {
    }

    @Test
    public void stringTest() throws InterruptedException {
        System.out.println("===========begin==========");

        String redisKey = "test.tang.redis.key";
        redisTemplate.opsForValue().set(redisKey, "tang");
        System.out.println(redisTemplate.opsForValue().get(redisKey));
        System.out.println("--------------------------");
        redisTemplate.opsForValue().set(redisKey.concat(".1"), "tang1", 1, TimeUnit.SECONDS);
        System.out.println(redisTemplate.opsForValue().get(redisKey.concat(".1")));
        Thread.sleep(2000);
        System.out.println(redisTemplate.opsForValue().get(redisKey.concat(".1")));
        System.out.println("--------------------------");
        redisTemplate.opsForValue().set(redisKey, "yongdong", 4);
        System.out.println(redisTemplate.opsForValue().get(redisKey));
        System.out.println("--------------------------");
        System.out.println(redisTemplate.opsForValue().setIfAbsent(redisKey.concat(".2"), "2"));
        System.out.println(redisTemplate.opsForValue().setIfAbsent(redisKey.concat(".2"), "2"));
        redisTemplate.delete(redisKey.concat(".2"));
        System.out.println("--------------------------");
        Map<String, String> maps = new HashMap<>(3);
        maps.put(redisKey.concat(".01"), "tang01");
        maps.put(redisKey.concat(".02"), "tang02");
        maps.put(redisKey.concat(".03"), "tang03");
        redisTemplate.opsForValue().multiSet(maps);
        Set<String> mapKeys = maps.keySet();
        List<String> lists = redisTemplate.opsForValue().multiGet(mapKeys);
        lists.stream().forEach(System.out::println);
        redisTemplate.delete(mapKeys);
        System.out.println("--------------------------");
        Map<String, String> params = new HashMap<>(3);
        params.put(redisKey.concat(".11"), "tang11");
        params.put(redisKey.concat(".12"), "tang12");
        params.put(redisKey.concat(".13"), "tang13");
        System.out.println(redisTemplate.opsForValue().multiSetIfAbsent(params));
        System.out.println(redisTemplate.opsForValue().multiSetIfAbsent(params));
        redisTemplate.delete(params.keySet());
        System.out.println("--------------------------");
        System.out.println(redisTemplate.opsForValue().getAndSet(redisKey, "zhaoxuezi"));
        System.out.println(redisTemplate.opsForValue().get(redisKey));
        System.out.println("--------------------------");
        //warning
        redisTemplate.opsForValue().increment(redisKey.concat(".3"), 1);
        System.out.println(redisTemplate.opsForValue().get(redisKey.concat(".3")));
        redisTemplate.opsForValue().increment(redisKey.concat(".3"), 1.1);
        System.out.println(redisTemplate.opsForValue().get(redisKey.concat(".3")));
        redisTemplate.delete(redisKey.concat(".3"));
        System.out.println("--------------------------");
        redisTemplate.opsForValue().append(redisKey.concat(".4"), "zhao");
        System.out.println(redisTemplate.opsForValue().get(redisKey.concat(".4")));
        redisTemplate.opsForValue().append(redisKey.concat(".4"), "xuezi");
        System.out.println(redisTemplate.opsForValue().get(redisKey.concat(".4")));
        System.out.println("--------------------------");
        System.out.println(redisTemplate.opsForValue().get(redisKey.concat(".4"), 0, 3));
        redisTemplate.delete(redisKey.concat(".4"));

        redisTemplate.delete(redisKey);

        System.out.println("============end===========");
    }

    @Test
    public void listTest() {
        System.out.println("===========begin==========");

        String redisKey = "test.tang.redis.list.key";

        redisTemplate.opsForList().leftPush(redisKey, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().leftPush(redisKey, "java", "c++");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().leftPushAll(redisKey, "c#", "python", "scala");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().leftPop(redisKey);
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.delete(redisKey);
        System.out.println("--------------------------");
        redisTemplate.opsForList().rightPush(redisKey, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().rightPush(redisKey, "java", "c++");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().rightPushAll(redisKey, "c#", "python", "scala");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().rightPop(redisKey);
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.delete(redisKey);
        System.out.println("--------------------------");
        List<String> lists = Lists.newArrayList("java", "c++", "c#", "python", "scala");
        redisTemplate.opsForList().leftPushAll(redisKey, lists);
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.delete(redisKey);
        System.out.println("--------------------------");
        redisTemplate.opsForList().rightPushAll(redisKey, lists);
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().trim(redisKey, 1, -1);
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        System.out.println(redisTemplate.opsForList().size(redisKey));
        redisTemplate.delete(redisKey);
        System.out.println("--------------------------");
        redisTemplate.opsForList().leftPushIfPresent(redisKey, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().leftPush(redisKey, "python");
        redisTemplate.opsForList().leftPushIfPresent(redisKey, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.delete(redisKey);
        System.out.println("--------------------------");
        redisTemplate.opsForList().rightPushIfPresent(redisKey, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().rightPush(redisKey, "python");
        redisTemplate.opsForList().rightPushIfPresent(redisKey, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.delete(redisKey);
        System.out.println("--------------------------");
        redisTemplate.opsForList().rightPushAll(redisKey, lists);
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().set(redisKey, 2, "go");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.delete(redisKey);
        System.out.println("--------------------------");
        List<String> newLists = Lists.newArrayList("java", "python", "java", "go", "java", "scala", "java");
        redisTemplate.opsForList().rightPushAll(redisKey, newLists);
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        /**
         * Long remove(K key, long count, Object value);
         * 从存储在键中的列表中删除等于值的元素的第一个计数事件。计数参数以下列方式影响操作：
         * count> 0：删除等于从头到尾移动的值的元素。
         * count <0：删除等于从尾到头移动的值的元素。
         * count = 0：删除等于value的所有元素。
         */
        redisTemplate.opsForList().remove(redisKey, 1, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().remove(redisKey, -1, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.opsForList().remove(redisKey, 0, "java");
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        redisTemplate.delete(redisKey);
        System.out.println("--------------------------");
        redisTemplate.opsForList().rightPushAll(redisKey, lists);
        System.out.println(redisTemplate.opsForList().range(redisKey, 0, -1));
        System.out.println(redisTemplate.opsForList().index(redisKey, 3));
        redisTemplate.delete(redisKey);

        System.out.println("============end===========");
    }


    @Test
    public void hashTest() {
        System.out.println("===========begin==========");

        String redisKey = "test.tang.redis.hash.key";
        redisTemplate.opsForHash().put(redisKey, "myName", "tangyongdong");
        redisTemplate.opsForHash().put(redisKey, "myLove", "zhaoxuezi");
        System.out.println(redisTemplate.opsForHash().entries(redisKey));

        System.out.println(redisTemplate.opsForHash().putIfAbsent(redisKey, "myName", "tangyongdong"));
        System.out.println(redisTemplate.opsForHash().putIfAbsent(redisKey, "myAge", "27"));
        System.out.println(redisTemplate.opsForHash().entries(redisKey));

        redisTemplate.opsForHash().delete(redisKey, "myAge");
        System.out.println(redisTemplate.opsForHash().entries(redisKey));

        System.out.println(redisTemplate.opsForHash().hasKey(redisKey, "myLove"));
        System.out.println(redisTemplate.opsForHash().hasKey(redisKey, "myAge"));

        System.out.println(redisTemplate.opsForHash().get(redisKey, "myLove"));

        List<Object> keys = Lists.newArrayList("myName", "myLove");
        System.out.println(redisTemplate.opsForHash().multiGet(redisKey, keys));

        System.out.println(redisTemplate.opsForHash().putIfAbsent(redisKey, "myAge", "27"));
        System.out.println(redisTemplate.opsForHash().increment(redisKey, "myAge", 1));
        System.out.println(redisTemplate.opsForHash().increment(redisKey, "myAge", 1.1));

        System.out.println(redisTemplate.opsForHash().keys(redisKey));

        System.out.println(redisTemplate.opsForHash().size(redisKey));

        redisTemplate.delete(redisKey);

        Map<String, Object> params = new HashMap<>();
        params.put("myName", "tangyongdong");
        params.put("myLove", "zhaoxuezi");
        params.put("myAge", "27");
        redisTemplate.opsForHash().putAll(redisKey, params);

        System.out.println(redisTemplate.opsForHash().values(redisKey));
        System.out.println(redisTemplate.opsForHash().entries(redisKey));

        Cursor<Map.Entry<Object, Object>> cursor = redisTemplate.opsForHash().scan(redisKey, ScanOptions.NONE);
        while (cursor.hasNext()) {
            Map.Entry<Object, Object> entry = cursor.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        redisTemplate.delete(redisKey);

        System.out.println("============end===========");
    }


    @Test
    public void setTest() {
        System.out.println("===========begin==========");

        String redisKey = "test.tang.redis.set.key";
        System.out.println(redisTemplate.opsForSet().add(redisKey, "tangyongdong", "zhaoxuezi", "zhangsan"));
        System.out.println(redisTemplate.opsForSet().members(redisKey));
        redisTemplate.delete(redisKey);

        String[] strs = new String[]{"tangyongdong", "zhaoxuezi", "zhangsan"};
        System.out.println(redisTemplate.opsForSet().add(redisKey, strs));
        System.out.println(redisTemplate.opsForSet().members(redisKey));

        redisTemplate.opsForSet().remove(redisKey, "zhangsan");
        System.out.println(redisTemplate.opsForSet().members(redisKey));

        redisTemplate.opsForSet().pop(redisKey);
        System.out.println(redisTemplate.opsForSet().members(redisKey));
        redisTemplate.delete(redisKey);

        System.out.println(redisTemplate.opsForSet().add(redisKey, strs));
        redisTemplate.opsForSet().move(redisKey, "zhangsan", redisKey.concat(".1"));
        System.out.println(redisTemplate.opsForSet().members(redisKey));
        System.out.println(redisTemplate.opsForSet().members(redisKey.concat(".1")));

        System.out.println(redisTemplate.opsForSet().isMember(redisKey, "tangyongdong"));
        System.out.println(redisTemplate.opsForSet().isMember(redisKey, "zhangsan"));
        redisTemplate.delete(redisKey);
        redisTemplate.delete(redisKey.concat(".1"));

        String[] newStrs = new String[]{"tangyongdong", "zhaoxuezi", "lisi"};
        System.out.println(redisTemplate.opsForSet().add(redisKey, strs));
        System.out.println(redisTemplate.opsForSet().add(redisKey.concat(".1"), newStrs));
        System.out.println(redisTemplate.opsForSet().intersect(redisKey, redisKey.concat(".1")));
        System.out.println(redisTemplate.opsForSet().intersectAndStore(redisKey, redisKey.concat(".1"), redisKey.concat(".2")));
        System.out.println(redisTemplate.opsForSet().members(redisKey.concat(".2")));
        redisTemplate.delete(redisKey.concat(".2"));

        System.out.println(redisTemplate.opsForSet().union(redisKey, redisKey.concat(".1")));
        System.out.println(redisTemplate.opsForSet().unionAndStore(redisKey, redisKey.concat(".1"), redisKey.concat(".2")));
        System.out.println(redisTemplate.opsForSet().members(redisKey.concat(".2")));
        redisTemplate.delete(redisKey.concat(".2"));

        System.out.println(redisTemplate.opsForSet().difference(redisKey, redisKey.concat(".1")));
        System.out.println(redisTemplate.opsForSet().differenceAndStore(redisKey, redisKey.concat(".1"), redisKey.concat(".2")));
        System.out.println(redisTemplate.opsForSet().members(redisKey.concat(".2")));
        redisTemplate.delete(redisKey);
        redisTemplate.delete(redisKey.concat(".1"));
        redisTemplate.delete(redisKey.concat(".2"));

        System.out.println(redisTemplate.opsForSet().add(redisKey, strs));
        System.out.println(redisTemplate.opsForSet().randomMember(redisKey));
        System.out.println(redisTemplate.opsForSet().distinctRandomMembers(redisKey, 3));
        System.out.println(redisTemplate.opsForSet().randomMembers(redisKey, 3));

        Cursor<String> cursor = redisTemplate.opsForSet().scan(redisKey, ScanOptions.NONE);
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        redisTemplate.delete(redisKey);

        System.out.println("============end===========");
    }

    @Test
    public void zSetTest() {
        System.out.println("===========begin==========");

        String redisKey = "test.tang.redis.zSet.key";

        System.out.println(redisTemplate.opsForZSet().add(redisKey, "tangyongdong", 27));
        System.out.println(redisTemplate.opsForZSet().add(redisKey, "zhaoxuezi", 26));
        System.out.println(redisTemplate.opsForZSet().score(redisKey, "tangyongdong"));
        System.out.println(redisTemplate.opsForZSet().range(redisKey, 0, -1));

        ZSetOperations.TypedTuple<String> tuple1 = new DefaultTypedTuple<>("zhangsan", 18.0);
        ZSetOperations.TypedTuple<String> tuple2 = new DefaultTypedTuple<>("lisi", 20.0);
        Set<ZSetOperations.TypedTuple<String>> tuples = new HashSet<>(2);
        tuples.add(tuple1);
        tuples.add(tuple2);
        System.out.println(redisTemplate.opsForZSet().add(redisKey, tuples));
        System.out.println(redisTemplate.opsForZSet().range(redisKey, 0, -1));

        System.out.println(redisTemplate.opsForZSet().remove(redisKey, "lisi"));
        System.out.println(redisTemplate.opsForZSet().range(redisKey, 0, -1));

        System.out.println(redisTemplate.opsForZSet().incrementScore(redisKey, "zhangsan", 20.0));
        System.out.println(redisTemplate.opsForZSet().range(redisKey, 0, -1));
        Set<ZSetOperations.TypedTuple<String>> typedTuples = redisTemplate.opsForZSet().rangeWithScores(redisKey, 0, -1);
        Iterator<ZSetOperations.TypedTuple<String>> iterator = typedTuples.iterator();
        while (iterator.hasNext()) {
            ZSetOperations.TypedTuple<String> typedTuple = iterator.next();
            System.out.println(typedTuple.getValue() + ":" + typedTuple.getScore());
        }
        System.out.println(redisTemplate.opsForZSet().rangeByScore(redisKey, 10, 30));

        Set<ZSetOperations.TypedTuple<String>> typedTuples2 = redisTemplate.opsForZSet().rangeByScoreWithScores(redisKey, 10, 30);
        Iterator<ZSetOperations.TypedTuple<String>> iterator2 = typedTuples2.iterator();
        while (iterator2.hasNext()) {
            ZSetOperations.TypedTuple<String> typedTuple = iterator2.next();
            System.out.println(typedTuple.getValue() + ":" + typedTuple.getScore());
        }

        System.out.println(redisTemplate.opsForZSet().add(redisKey, "lisi", 32.0));
        System.out.println(redisTemplate.opsForZSet().rangeByScore(redisKey, 10, 35, 0, 1));

        System.out.println(redisTemplate.opsForZSet().rank(redisKey, "tangyongdong"));
        System.out.println(redisTemplate.opsForZSet().rank(redisKey, "zhaoxuezi"));

        System.out.println(redisTemplate.opsForZSet().reverseRank(redisKey, "tangyongdong"));
        System.out.println(redisTemplate.opsForZSet().reverseRank(redisKey, "zhaoxuezi"));

        //reverse开头的其他方法和 之前的方法一致，只是排序方式不同（从大到小），在此不加赘述

        System.out.println(redisTemplate.opsForZSet().count(redisKey, 10, 30));
        System.out.println(redisTemplate.opsForZSet().zCard(redisKey));

        System.out.println(redisTemplate.opsForZSet().range(redisKey, 0, -1));
        System.out.println(redisTemplate.opsForZSet().removeRange(redisKey, 2, 3));
        System.out.println(redisTemplate.opsForZSet().range(redisKey, 0, -1));

        System.out.println(redisTemplate.opsForZSet().add(redisKey, "zhangsan", 35.0));
        System.out.println(redisTemplate.opsForZSet().add(redisKey, "lisi", 32.0));
        System.out.println(redisTemplate.opsForZSet().range(redisKey, 0, -1));
        System.out.println(redisTemplate.opsForZSet().removeRangeByScore(redisKey, 30, 40));
        System.out.println(redisTemplate.opsForZSet().range(redisKey, 0, -1));

        redisTemplate.delete(redisKey);

        System.out.println(redisTemplate.opsForZSet().add(redisKey, "zset-1", 1.0));
        System.out.println(redisTemplate.opsForZSet().add(redisKey, "zset-2", 2.0));
        System.out.println(redisTemplate.opsForZSet().add(redisKey, "zset-3", 3.0));
        System.out.println(redisTemplate.opsForZSet().add(redisKey, "zset-4", 6.0));

        System.out.println(redisTemplate.opsForZSet().add(redisKey.concat(".1"), "zset-2", 2.0));
        System.out.println(redisTemplate.opsForZSet().add(redisKey.concat(".1"), "zset-3", 3.0));
        System.out.println(redisTemplate.opsForZSet().add(redisKey.concat(".1"), "zset-4", 6.0));
        System.out.println(redisTemplate.opsForZSet().add(redisKey.concat(".1"), "zset-5", 7.0));

        System.out.println(redisTemplate.opsForZSet().unionAndStore(redisKey, redisKey.concat(".1"), redisKey.concat(".2")));

        Set<ZSetOperations.TypedTuple<String>> typedTuples3 = redisTemplate.opsForZSet().rangeWithScores(redisKey.concat(".2"), 0, -1);
        Iterator<ZSetOperations.TypedTuple<String>> iterator3 = typedTuples3.iterator();
        while (iterator3.hasNext()) {
            ZSetOperations.TypedTuple<String> typedTuple = iterator3.next();
            System.out.println(typedTuple.getValue() + ":" + typedTuple.getScore());
        }

        System.out.println("============end===========");
    }
}
