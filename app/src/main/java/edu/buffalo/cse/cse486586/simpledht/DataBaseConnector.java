package edu.buffalo.cse.cse486586.simpledht;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by pragya on 2/16/15.
 */
public class DataBaseConnector extends SQLiteOpenHelper {

    public static final String DATABASE_NAME="test";
    private static final int DATABASE_VERSION=1;

    public static final String TABLE_NAME="message";
    public static final String TABLE_COLUMN1="key";
    public static final String TABLE_COLUMN2="value";

    private static final String DATABASE_CREATE="CREATE TABLE "+TABLE_NAME+"( "+TABLE_COLUMN1+" TEXT PRIMARY KEY, "+
            TABLE_COLUMN2+" TEXT NOT NULL );";


    public DataBaseConnector(Context context){
        super(context,DATABASE_NAME,null,DATABASE_VERSION);
    }


    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(DATABASE_CREATE);
    }
    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP table if exists "+TABLE_NAME);
        onCreate(db);
    }
}
