import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Wordsum {
    private String word;
    private Integer count;

    @Override
    public String toString() {
        return word + ", " + count  ;
    }
}
