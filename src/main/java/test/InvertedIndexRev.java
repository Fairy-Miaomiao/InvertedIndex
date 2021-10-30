package test;

public class InvertedIndexRev {
    private String key;
    private String value;
    public InvertedIndexRev(String count, String word){
        this.key = count;
        this.value = word;
    }
   /* public int compareTo(InvertedIndexRev o) {
        if(key == o.getKey()){
            return value.compareTo(o.getValue());
        }else {
            return key - o.getKey();
        }
    }*/
    public String getValue() {
        return value;
    }
    public String getKey() {
        return key;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
