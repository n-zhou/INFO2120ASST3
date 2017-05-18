import javax.swing.JFrame;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

public class GUI extends JFrame{

  /**
  *
  */
  private static final long serialVersionUID = 1L;


  JPasswordField password;
  GridBagLayout gridbag;
  GridBagConstraints c;

  public GUI(){
    super("George Michael 2.0");    
    setLayout(new GridBagLayout());
    this.c = new GridBagConstraints();
    this.password = new JPasswordField();
    c.gridx = 0;
    c.gridy = 0;
    add(this.password, c);
    setSize(1920, 1080);
    setResizable(true);
    setVisible(true);
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
  }

  public static void main(String[] args){
    new GUI();
  }

}
