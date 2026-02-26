#include "ob_imlane_plugin.h"
#include "dbend/c/imlane_dbend.hpp"

// #include "identity_arrow_converter.hpp"

namespace dbend = IMLane::DBEnd;

namespace oceanbase {

    namespace sql {
        class IMLanePlugin::Impl {
            public:
                Impl() {
                    // Constructor implementation
                    dbend_ctx_ = new dbend::DBEndContext();
                }
                ~Impl() {
                    // Destructor implementation
                    delete dbend_ctx_;
                }
            public:
                // Private members
                dbend::DBEndContext* dbend_ctx_;
        };

        IMLanePlugin::IMLanePlugin()
            : impl_(new Impl()) {}

        IMLanePlugin::~IMLanePlugin() {
            delete impl_;
        }

        void IMLanePlugin::Setup() {
            impl_->dbend_ctx_->Setup();
        }
    }
}