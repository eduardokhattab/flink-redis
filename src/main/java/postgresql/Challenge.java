package postgresql;

public class Challenge {
    public Challenge(Long id, String title, Boolean completed) {
        this.id = id;
        this.title = title;
        this.completed = completed;
    }

    public Long id;
    public String title;
    public Boolean completed;
}
