
#include <stdio.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include "org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem.h"

/* Header for class org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem */

/*
 * Class:     org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem
 * Method:    getUserName
 * Signature: (I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem_getUserName
  (JNIEnv *env, jobject obj, jint id) {
    struct passwd *pwd = getpwuid(id);
    if (pwd == NULL) {
      jstring name = (*env)->NewStringUTF(env, "nobody");
      return name;
    } else {
      jstring name = (*env)->NewStringUTF(env, pwd->pw_name);
      return name;
    }
  }

/*
 * Class:     org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem
 * Method:    getGroupName
 * Signature: (I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem_getGroupName
  (JNIEnv *env, jobject obj, jint id) {
    struct group *grp = getgrgid(id);
    if (grp == NULL) {
      jstring name = (*env)->NewStringUTF(env, "nobody");
      return name;
    } else {
      jstring name = (*env)->NewStringUTF(env, grp->gr_name);
      return name;
    }
  }

/*
 * Class:     org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem
 * Method:    getUserID
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem_getUserID
  (JNIEnv *env, jobject obj, jstring userName) {
    struct passwd *pwd = getpwnam((*env)->GetStringUTFChars(env, userName, 0));
    if (pwd == NULL) {
      return -1;
    } else {
      return pwd->pw_uid;
    }
  }

/*
 * Class:     org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem
 * Method:    getGroupID
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem_getGroupID
  (JNIEnv *env, jobject obj, jstring groupName) {
    struct group *grp = getgrnam((*env)->GetStringUTFChars(env, groupName, 0));
    if (grp == NULL) {
      return -1;
    } else {
      return grp->gr_gid;
    }
  }
