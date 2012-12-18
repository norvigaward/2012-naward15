package cwi.commoncrawl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.LinkedList;

public class RandomSampler {
        public static <T> ArrayList<T> randomSample(ArrayList<T> items, int m){
                        ArrayList<T> res = new ArrayList<T>();
                    int n = items.size();
                    if (m > n/2){ // The optimization
                        ArrayList<T> negativeSet = randomSample(items, n-m);
                        for(T item : items){
                            if (!negativeSet.contains(item))
                                res.add(item);
                        }
                    }else{ // The main loop
                        Random rnd = new Random();
                        while(res.size() < m){
                            int randPos = rnd.nextInt(n);
                            if (!res.contains(items.get(randPos)))
                            {
                               res.add(items.get(randPos));
                            }
                        }
                    }
                    return res;
                }

}