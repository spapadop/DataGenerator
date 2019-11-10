import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.person.Person;
import com.devskiller.jfairy.producer.person.PersonProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.*;

import static org.apache.kafka.common.utils.Utils.sleep;


public class PersonProducer {
    private final String topic_name = "person";
    private Person person;
    private final char DEFAULT_SEPARATOR = '|';
    private PrintWriter writer;
    private String type; // CSV or JSON
    private Properties props;
    private KafkaProducer producer;
    private Fairy fairy;


    public PersonProducer() {
        fairy = Fairy.create();
        this.type = "csv";
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, topic_name);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        createPersonsEternally();
    }

    private void createPersonsEternally() {
        producer = new KafkaProducer(props);
        ProducerRecord<Long, String> record;
        while(true) {
            person = fairy.person(PersonProperties.ageBetween(20,60));
            String data = personInfo();
            System.out.println(data);
            record = new ProducerRecord<Long, String>(topic_name, data);
            producer.send(record);
            System.out.println("Sent 1 person to topic " + topic_name + " with username: " + data.split("\\|")[0]);
            sleep(5000);
        }
    }

    private String personHeader(){
        return  "username"      + DEFAULT_SEPARATOR +
                "firstName"     + DEFAULT_SEPARATOR +
                "lastName"      + DEFAULT_SEPARATOR +
                "dateOfBirth"   + DEFAULT_SEPARATOR +
                "company"       + DEFAULT_SEPARATOR +
                "sex"           + DEFAULT_SEPARATOR +
                "nationality"   + DEFAULT_SEPARATOR +
                "age"         ;
    }

    private String personInfo(){
        return    person.getPassportNumber()        + DEFAULT_SEPARATOR //username
                + person.getFirstName()             + DEFAULT_SEPARATOR //first name
                + person.getLastName()              + DEFAULT_SEPARATOR //last name
                + person.getDateOfBirth()           + DEFAULT_SEPARATOR //date of birth
                + person.getCompany().getName()     + DEFAULT_SEPARATOR //company
                + person.getSex()                   + DEFAULT_SEPARATOR //sex
                + person.getNationality().getCode() + DEFAULT_SEPARATOR //nationality
                + person.getAge();                                      //age
    }

    /**
     * Creates a file with 1000 persons.
     * @throws FileNotFoundException
     */
    private void createPersonsFile() throws FileNotFoundException {
        writer = new PrintWriter("src/main/resources/persons" + 1000 + "." + type);
        writer.println(personHeader());
        for (int i = 0; i < 1000; i++) {
            person = fairy.person(PersonProperties.ageBetween(20,60));
            writer.println(personInfo());
        }
        writer.close();
    }


}