package controllers

import play.api._
import play.api.mvc._
import java.io.File
import org.apache.mahout.cf.taste.impl.model.mongodb.MongoDBDataModel
import org.apache.mahout.cf.taste.similarity.UserSimilarity
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood
import org.apache.mahout.cf.taste.common.Weighting
import org.apache.mahout.cf.taste.impl.model.{GenericUserPreferenceArray, PlusAnonymousConcurrentUserDataModel}

object Application extends Controller {
  val model = new MongoDBDataModel();
  val plusModel = new PlusAnonymousConcurrentUserDataModel(model, 5);

  def index = Action {
    val similarity = new PearsonCorrelationSimilarity(model, Weighting.WEIGHTED)

    val anonymousUserID = plusModel.takeAvailableUser();


    //preferences for anonymous user
    val tempPrefs = new GenericUserPreferenceArray(10)
    tempPrefs.setUserID(0, anonymousUserID);
    tempPrefs.setItemID(0, 12);
    plusModel.setTempPrefs(tempPrefs, anonymousUserID);




    val neighborhood = new NearestNUserNeighborhood(5, similarity, model)
    val neighbors = neighborhood.getUserNeighborhood(anonymousUserID)

    plusModel.releaseUser(anonymousUserID);
    Ok(views.html.index("Your new application is ready." + neighbors.size.toString))
  }

  def clusters = Action {
    val similarity = new PearsonCorrelationSimilarity(model, Weighting.WEIGHTED)

    val anonymousUserID = plusModel.takeAvailableUser();


    //preferences for anonymous user
    val tempPrefs = new GenericUserPreferenceArray(10)
    tempPrefs.setUserID(0, anonymousUserID);
    tempPrefs.setItemID(0, 12);
    plusModel.setTempPrefs(tempPrefs, anonymousUserID);




    val neighborhood = new NearestNUserNeighborhood(5, similarity, model)
    val neighbors = neighborhood.getUserNeighborhood(anonymousUserID)

    plusModel.releaseUser(anonymousUserID);
    Ok(views.html.index("Your new application is ready." + neighbors.size.toString))
  }

}