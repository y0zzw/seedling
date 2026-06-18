package rbac

import (
	cfg "v-gnms/config"
	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/models"

	"github.com/casbin/casbin/v2"
	mongodbadapter "github.com/casbin/mongodb-adapter/v3"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/crypto/bcrypt"
)

var CasbinEnforcer *casbin.Enforcer
var RBACActions []map[string]interface{}

func InitializeCasbinRBAC() {
	// Initialize a MongoDB adapter and use it in a Casbin enforcer:
	// The adapter will use the database named "casbin".
	// If it doesn't exist, the adapter will create it automatically.
	a, err := mongodbadapter.NewAdapter(cfg.GetConfigValue("MONGODB_URI")) // Your MongoDB URL.
	if err != nil {
		panic(err)
	}
	// Or you can use an existing DB "abc" like this:
	// The adapter will use the table named "casbin_rule".
	// If it doesn't exist, the adapter will create it automatically.
	// a := mongodbadapter.NewAdapter("127.0.0.1:27017/abc")

	CasbinEnforcer, err = casbin.NewEnforcer("rbac_model.conf", a)
	if err != nil {
		panic(err)
	}

	CasbinEnforcer.EnableLog(true)
	CasbinEnforcer.LoadPolicy()
	// e.AddRoleForUser("alice", "admin")
	// CasbinEnforcer.Enforce("alice", "data1", "read")
	// e.SavePolicy()
}

func SetUserRole(user *User, role *Role) {
	CasbinEnforcer.DeleteRolesForUser(user.ID)
	CasbinEnforcer.AddRoleForUser(user.ID, role.IdentName)
	CasbinEnforcer.LoadPolicy()
}

func DeleteUserRole(user *User) {
	CasbinEnforcer.DeleteRolesForUser(user.ID)
	CasbinEnforcer.LoadPolicy()
}

func CheckAndInitializeSuperuserPermissions() {
	role, err := FindSingleByField[Role](RolesCollection, "ident_name", "superuser", nil)
	if err != nil {
		isActive := true
		role = Role{
			ID:          GenerateUUIDv7(),
			IdentName:   "superuser",
			DisplayName: "Superuser",
			IsActive:    &isActive,
		}

		role, _ = Create[Role](RolesCollection, role)

		// for _, action := range RBACActions {
		// 	CasbinEnforcer.AddPermissionForUser(role.IdentName, action["identifier"].(string))
		// }

	}
	CasbinEnforcer.AddPermissionForUser("superuser", "superuser")

	CheckAndInitializeDefaultUser(role)
}

func CheckAndInitializeDefaultUser(role Role) {
	filter := bson.D{
		bson.E{"username", "superuser"},
		bson.E{"email", "superuser@vgnms.com"},
	}

	count, err := Count[User](UsersCollection, filter, nil)

	shouldCreateDefaultUser := false

	if err != nil {
		shouldCreateDefaultUser = true
	}
	if count == 0 {
		shouldCreateDefaultUser = true
	}

	if shouldCreateDefaultUser {
		user := User{
			ID:        GenerateUUIDv7(),
			Username:  "superuser",
			FirstName: "Superuser",
			LastName:  "Superuser",
			Email:     "superuser@vgnms.com",
			Password:  "vgnms",
			RoleID:    role.ID,
		}

		isActive := true
		user.IsActive = &isActive

		// Hash and update the password
		hashed, _ := bcrypt.GenerateFromPassword([]byte(user.Password), 12)
		user.Password = string(hashed)

		user, err = Create[User](UsersCollection, user)

		SetUserRole(&user, &role)
	}
}
